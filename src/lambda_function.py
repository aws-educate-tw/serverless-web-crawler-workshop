import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

from config.database import get_connection_pool
from repositories import (
    CrawlerExecutionsRepository,
    QuestionsRepository,
    QuestionTagsRepository,
    TagsRepository,
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
REPOST_URL = "https://repost.aws/questions?view=all&sort=recent"
REPOST_URL_ZH = "https://repost.aws/zh-Hant/questions?view=all&sort=recent"

# AWS Service Categories
AWS_SERVICES = {
    "compute": {
        "ec2",
        "lambda",
        "ecs",
        "eks",
        "fargate",
        "batch",
        "lightsail",
        "elastic-beanstalk",
    },
    "storage": {"s3", "ebs", "efs", "fsx", "storage-gateway"},
    "database": {"rds", "dynamodb", "aurora", "redshift", "documentdb", "elasticache"},
    "networking": {"vpc", "route53", "cloudfront", "api-gateway", "direct-connect"},
    "security": {"iam", "cognito", "kms", "waf", "shield", "security-hub"},
    "analytics": {"athena", "emr", "elasticsearch", "kinesis", "quicksight"},
    "integration": {"sns", "sqs", "eventbridge", "step-functions"},
    "management": {"cloudwatch", "cloudformation", "organizations", "systems-manager"},
    "developer-tools": {"codecommit", "codebuild", "codedeploy", "codepipeline"},
    "machine-learning": {"sagemaker", "comprehend", "rekognition", "polly", "textract"},
}

# Question Type Patterns
QUESTION_TYPES = {
    "error": {"error", "exception", "failed", "trouble", "issue", "problem", "debug"},
    "how_to": {"how to", "how do i", "way to", "guide", "tutorial"},
    "best_practice": {
        "best practice",
        "recommend",
        "optimal",
        "better way",
        "improvement",
    },
    "comparison": {"vs", "versus", "compare", "difference between", "choose between"},
    "configuration": {"configure", "setup", "setting", "configuration", "parameter"},
    "performance": {
        "performance",
        "optimization",
        "slow",
        "faster",
        "latency",
        "throughput",
    },
    "cost": {"cost", "pricing", "bill", "expense", "budget"},
    "security": {"security", "permission", "access", "authentication", "authorization"},
}

# Environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "repost-questions/")
USER_AGENT = os.environ["USER_AGENT"]

# Initialize S3 client
try:
    s3_client = boto3.client("s3")
    # Initialize repositories
    pool = get_connection_pool()
    questions_repo = QuestionsRepository(pool)
    tags_repo = TagsRepository(pool)
    question_tags_repo = QuestionTagsRepository(pool)
    crawler_executions_repo = CrawlerExecutionsRepository(pool)
except Exception as e:
    logger.error("Failed to initialize clients: %s", str(e))
    raise

# HTTP Headers
headers = {"User-Agent": USER_AGENT}


def process_question(question_data: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
    """
    Process a single question and save to database

    Args:
        question_data: Question data dictionary

    Returns:
        Tuple of (success status, question ID if successful)
    """
    try:
        # Create or update question
        question_id = questions_repo.create_or_update(question_data)
        if not question_id:
            return False, None

        # Process tags
        if question_data.get("tags"):
            # Create or get tag IDs
            tag_ids = []
            for tag_name in question_data["tags"]:
                tag_id = tags_repo.create_or_get(tag_name)
                if tag_id:
                    tag_ids.append(tag_id)

            # Update question tags
            if tag_ids:
                question_tags_repo.update_question_tags(question_id, set(tag_ids))

        return True, question_id

    except Exception as e:
        logger.error("Error processing question data: %s", str(e))
        return False, None


def categorize_aws_services(tags: List[str], text: str) -> Set[str]:
    """
    Categorize AWS services mentioned in tags and text

    Args:
        tags (List[str]): List of question tags
        text (str): Question text

    Returns:
        Set[str]: Set of AWS service categories
    """
    found_categories = set()
    combined_text = " ".join(tags + [text.lower()])

    for category, services in AWS_SERVICES.items():
        if any(service in combined_text for service in services):
            found_categories.add(category)

    return found_categories


def identify_question_types(text: str) -> Set[str]:
    """
    Identify question types based on text patterns

    Args:
        text (str): Question text

    Returns:
        Set[str]: Set of identified question types
    """
    found_types = set()
    text_lower = text.lower()

    for qtype, patterns in QUESTION_TYPES.items():
        if any(pattern in text_lower for pattern in patterns):
            found_types.add(qtype)

    return found_types


def get_question_content(question_url: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the full content of a question

    Args:
        question_url (str): URL of the question

    Returns:
        Optional[Dict[str, Any]]: Question details if successful, None otherwise
    """
    try:
        logger.info("Fetching question content from %s", question_url)
        response = requests.get(question_url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Get question description
        description = None

        # Try to find the description content
        description_div = soup.find("div", {"data-test": "question-description"})
        if description_div:
            # Find paragraphs within the custom-md-style div
            content_div = description_div.find("div", class_="custom-md-style")
            if content_div:
                # Get the text of all paragraphs
                paragraphs = content_div.find_all("p")
                description = "\n".join(p.text.strip() for p in paragraphs)
                if not description:
                    # If no paragraphs are found, directly get the text of the div
                    description = content_div.text.strip()

        if not description:
            logger.warning("No description found for question: %s", question_url)

        return {
            "description": description,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error(
            "Error fetching question content from %s: %s", question_url, str(e)
        )
        return None


def fetch_questions(url: str) -> List[Dict[str, Any]]:
    """
    Fetch questions from AWS re:Post
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

                # Get tags
                tag_elements = question.find_all(
                    "span",
                    class_=[
                        "ant-tag",
                        "NavigableTag_tag__BmXT_",
                        "CustomTag_tag__kXm6J",
                    ],
                )
                tags = []
                for tag in tag_elements:
                    if "CustomTag_accepted__VKlHK" not in tag.get("class", []):
                        tag_text = tag.text.strip()
                        if tag_text:
                            tags.append(tag_text)

                # Get statistics (votes, views, answers)
                stats_elements = question.find_all(
                    "div", class_="AnswersVotesViews_count__9rLX_"
                )

                # Initialize default values
                answers_count = "0"
                vote_count = "0"
                view_count = "0"

                # Usually the order is: answers, votes, views
                if stats_elements and len(stats_elements) >= 3:
                    answers_count = stats_elements[0].text.strip()
                    vote_count = stats_elements[1].text.strip()
                    view_count = stats_elements[2].text.strip()

                # Get detailed question content
                question_details = get_question_content(question_url)

                # Create base question data
                question_data = {
                    "url": question_url,
                    "title": question_text,
                    "has_accepted_answer": question_accepted_tag is not None,
                    "tags": tags,
                    "timestamp": timestamp,
                    "language": "zh-Hant" if "/zh-Hant/" in url else "en",
                    "answers_count": int(answers_count),
                    "vote_count": int(vote_count),
                    "view_count": int(view_count),
                    "crawled_at": datetime.now(timezone.utc).isoformat(),
                }

                # Add description if available
                if question_details and question_details.get("description"):
                    question_data["description"] = question_details["description"]

                processed_questions.append(question_data)
                logger.debug(
                    "Processed question: %s with description length: %s",
                    question_text,
                    (
                        len(question_details.get("description", ""))
                        if question_details
                        else 0
                    ),
                )

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


def update_execution_log(execution_info: Dict[str, Any]) -> bool:
    """
    Update the crawler execution log in S3

    Args:
        execution_info (Dict[str, Any]): Information about the current execution

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Log file path
        log_file = f"{S3_PREFIX}crawler_execution_log.json"

        # Try to get existing log
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=log_file)
            log_data = json.loads(response["Body"].read().decode("utf-8"))
            executions = log_data.get("executions", [])
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                executions = []
            else:
                raise

        # Add new execution record
        executions.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "questions_processed": execution_info["total_questions"],
                "english_questions": execution_info["english_questions"],
                "chinese_questions": execution_info["chinese_questions"],
                "output_file": execution_info["output_file"],
                "status": execution_info["status"],
                "error_message": execution_info.get("error_message"),
                "duration_ms": execution_info.get("duration_ms"),
            }
        )

        # Keep only the last 1000 executions
        if len(executions) > 1000:
            executions = executions[-1000:]

        # Create updated log data
        log_data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_executions": len(executions),
            "executions": executions,
        }

        # Save updated log
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=log_file,
            Body=json.dumps(log_data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )

        logger.info("Successfully updated execution log")
        return True

    except Exception as e:
        logger.error("Error updating execution log: %s", str(e))
        return False


def save_to_s3(
    questions: List[Dict[str, Any]], start_time: datetime
) -> Tuple[bool, str]:
    """
    Save questions data to S3 in JSON format

    Args:
        questions (List[Dict[str, Any]]): List of question data to save
        start_time (datetime): Execution start time

    Returns:
        Tuple[bool, str]: (Success status, Output filename)
    """
    if not questions:
        logger.warning("No questions to save")
        return False, ""

    try:
        # Create filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{S3_PREFIX}questions_{timestamp}.json"

        # Create metadata
        metadata = {
            "question_count": len(questions),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_urls": [REPOST_URL, REPOST_URL_ZH],
            "en_count": sum(1 for q in questions if q["language"] == "en"),
            "zh_count": sum(1 for q in questions if q["language"] == "zh-Hant"),
            "tags": list(
                set(tag for question in questions for tag in question.get("tags", []))
            ),
            "questions_with_accepted_answers": sum(
                1 for q in questions if q.get("has_accepted_answer", False)
            ),
            "execution_duration_ms": int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            ),
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
        return True, filename

    except Exception as e:
        logger.error("Unexpected error saving to S3: %s", str(e))
        return False, ""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function

    Args:
        event: Lambda event
        context: Lambda context

    Returns:
        Lambda response
    """
    start_time = datetime.now(timezone.utc)

    try:
        logger.info("Starting Lambda execution")

        # Fetch questions from both English and Traditional Chinese endpoints
        questions_en = fetch_questions(REPOST_URL)
        questions_zh = fetch_questions(REPOST_URL_ZH)

        # Combine all questions
        all_questions = questions_en + questions_zh

        # Process questions in database
        processed_count = 0
        for question in all_questions:
            success, _ = process_question(question)
            if success:
                processed_count += 1

        # Save to S3
        s3_success, output_file = save_to_s3(all_questions, start_time)

        # Record execution
        execution_data = {
            "start_time": start_time,
            "end_time": datetime.now(timezone.utc),
            "questions_processed": processed_count,
            "english_questions": len(questions_en),
            "chinese_questions": len(questions_zh),
            "status": "success" if s3_success else "error",
            "duration_ms": int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            ),
            "output_file": output_file,
        }

        crawler_executions_repo.create(execution_data)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Successfully processed and saved questions",
                    "total_questions": len(all_questions),
                    "processed_questions": processed_count,
                    "english_questions": len(questions_en),
                    "chinese_questions": len(questions_zh),
                    "output_file": output_file,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "duration_ms": execution_data["duration_ms"],
                }
            ),
        }

    except Exception as e:
        error_message = str(e)
        logger.error("Unexpected error in lambda_handler: %s", error_message)

        # Record error execution
        execution_data = {
            "start_time": start_time,
            "end_time": datetime.now(timezone.utc),
            "questions_processed": 0,
            "english_questions": 0,
            "chinese_questions": 0,
            "status": "error",
            "error_message": error_message,
            "duration_ms": int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            ),
            "output_file": "",
        }

        try:
            crawler_executions_repo.create(execution_data)
        except Exception as ex:
            logger.error("Failed to record error execution: %s", str(ex))

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "Internal server error", "message": error_message}
            ),
        }
