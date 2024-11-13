import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
REPOST_URL = "https://repost.aws/questions?view=all&sort=recent"
REPOST_URL_ZH = "https://repost.aws/zh-Hant/questions?view=all&sort=recent"

# Environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "repost-questions/")
USER_AGENT = os.environ["USER_AGENT"]

# Initialize S3 client
try:
    s3_client = boto3.client("s3")
except Exception as e:
    logger.error("Failed to initialize AWS client: %s", str(e))
    raise

# HTTP Headers
headers = {"User-Agent": USER_AGENT}


def fetch_questions(url: str) -> List[Dict[str, Any]]:
    """
    Fetch questions from AWS re:Post

    Args:
        url (str): The URL to fetch questions from

    Returns:
        List[Dict[str, Any]]: List of questions with their details
    """
    try:
        logger.info("Fetching questions from %s", url)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        question_list = soup.find("div", class_="ant-row ant-row-start")

        if not question_list:
            logger.warning("No question list found at %s", url)
            return []

        questions = question_list.find_all(
            "div", class_="QuestionCard_card__E3_x5 QuestionCard_grid__0e3xB"
        )
        processed_questions = []

        for question in questions:
            try:
                # Extract question details
                question_url = "https://repost.aws" + question.find("a")["href"]
                question_text = question.find("a").text.strip()
                question_accepted_tag = question.find(
                    "span",
                    class_="ant-tag CustomTag_tag__kXm6J CustomTag_accepted__VKlHK",
                )

                # Get timestamp if available
                timestamp_element = question.find(
                    "span", class_="QuestionCard_date__TUqqb"
                )
                timestamp = (
                    timestamp_element.text.strip() if timestamp_element else None
                )

                # Get tags if available
                tags_container = question.find(
                    "div", class_="QuestionCard_tagContainer__hXXd5"
                )
                tags = []
                if tags_container:
                    tag_elements = tags_container.find_all("span", class_="ant-tag")
                    tags = [tag.text.strip() for tag in tag_elements if tag]

                # Get vote count and view count if available
                vote_count_element = question.find(
                    "span", class_="QuestionCard_voteCount__DOYYL"
                )
                view_count_element = question.find(
                    "span", class_="QuestionCard_viewCount__lOPE5"
                )

                vote_count = (
                    vote_count_element.text.strip() if vote_count_element else "0"
                )
                view_count = (
                    view_count_element.text.strip() if view_count_element else "0"
                )

                # Create structured question data
                question_data = {
                    "url": question_url,
                    "text": question_text,
                    "has_accepted_answer": question_accepted_tag is not None,
                    "tags": tags,
                    "timestamp": timestamp,
                    "language": "zh-Hant" if "/zh-Hant/" in url else "en",
                    "vote_count": vote_count,
                    "view_count": view_count,
                    "crawled_at": datetime.now(timezone.utc).isoformat(),
                }

                processed_questions.append(question_data)
                logger.debug("Processed question: %s", question_text)

            except Exception as e:
                logger.error("Error processing individual question: %s", str(e))
                continue

        logger.info(
            "Successfully processed %d questions from %s", len(processed_questions), url
        )
        return processed_questions

    except requests.RequestException as e:
        logger.error("Error fetching questions from %s: %s", url, str(e))
        return []
    except Exception as e:
        logger.error(
            "Unexpected error while fetching questions from %s: %s", url, str(e)
        )
        return []


def save_to_s3(questions: List[Dict[str, Any]]) -> bool:
    """
    Save questions data to S3 in JSON format

    Args:
        questions (List[Dict[str, Any]]): List of question data to save

    Returns:
        bool: True if successful, False otherwise
    """
    if not questions:
        logger.warning("No questions to save")
        return False

    try:
        # Create filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{S3_PREFIX}questions_{timestamp}.json"

        # Create metadata
        metadata = {
            "question_count": len(questions),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_urls": [REPOST_URL, REPOST_URL_ZH],
        }

        # Create final data structure
        data = {"metadata": metadata, "questions": questions}

        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )

        logger.info(
            "Successfully saved %d questions to s3://%s/%s",
            len(questions),
            S3_BUCKET,
            filename,
        )
        return True

    except ClientError as e:
        logger.error("Error saving to S3: %s", str(e))
        return False
    except Exception as e:
        logger.error("Unexpected error saving to S3: %s", str(e))
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function

    Args:
        event (Dict[str, Any]): Lambda event
        context (Any): Lambda context

    Returns:
        Dict[str, Any]: Lambda response
    """
    try:
        logger.info("Starting Lambda execution")

        # Fetch questions from both English and Traditional Chinese endpoints
        questions_en = fetch_questions(REPOST_URL)
        questions_zh = fetch_questions(REPOST_URL_ZH)

        # Combine all questions
        all_questions = questions_en + questions_zh

        # Save to S3
        if save_to_s3(all_questions):
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Successfully processed and saved questions",
                        "total_questions": len(all_questions),
                        "english_questions": len(questions_en),
                        "chinese_questions": len(questions_zh),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                ),
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to save questions to S3"}),
            }

    except Exception as e:
        logger.error("Unexpected error in lambda_handler: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
