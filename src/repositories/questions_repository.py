# repositories/questions_repository.py
import logging
from typing import Any, Dict, List, Optional

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class QuestionsRepository(BaseRepository):
    def create_or_update(self, question_data: Dict[str, Any]) -> Optional[int]:
        """
        Create or update a question (update if exists)

        Args:
            question_data: Question data dictionary containing:
                - question_id: AWS re:Post question ID
                - title: Question title
                - description: Question description
                - language: Question language ('en' or 'zh-Hant')
                - url: Question URL
                - view_count: Number of views
                - vote_count: Number of votes
                - answers_count: Number of answers
                - has_accepted_answer: Whether has accepted answer
                - posted_at: Question posted time

        Returns:
            Question ID
        """
        # Extract question_id from URL if not provided
        if "question_id" not in question_data and "url" in question_data:
            question_data["question_id"] = question_data["url"].split("/")[-1]

        # Check if question exists
        existing_question = self.get_by_question_id(question_data["question_id"])

        if existing_question:
            self.update(existing_question["id"], question_data)
            return existing_question["id"]
        else:
            return self.create(question_data)

    def create(self, question_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new question

        Args:
            question_data: Question data dictionary

        Returns:
            New question ID
        """
        query = """
        INSERT INTO questions (
            question_id, title, description, language, url,
            view_count, vote_count, answers_count,
            has_accepted_answer, posted_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params = (
            question_data["question_id"],
            question_data["title"],
            question_data.get("description"),
            question_data["language"],
            question_data["url"],
            question_data.get("view_count", 0),
            question_data.get("vote_count", 0),
            question_data.get("answers_count", 0),
            question_data.get("has_accepted_answer", False),
            question_data.get("posted_at"),
        )

        return self._execute_write(query, params)

    def update(self, question_id: int, question_data: Dict[str, Any]) -> bool:
        """
        Update question data

        Args:
            question_id: Question ID
            question_data: Updated data

        Returns:
            True if successful
        """
        fields = []
        params = []

        # Map of field names and their values
        field_map = {
            "title": "title",
            "description": "description",
            "language": "language",
            "url": "url",
            "view_count": "view_count",
            "vote_count": "vote_count",
            "answers_count": "answers_count",
            "has_accepted_answer": "has_accepted_answer",
            "posted_at": "posted_at",
        }

        # Build update fields
        for key, field in field_map.items():
            if key in question_data:
                fields.append(f"{field} = %s")
                params.append(question_data[key])

        if not fields:
            return False

        # Add question_id to params
        params.append(question_id)

        query = f"UPDATE questions SET {', '.join(fields)} WHERE id = %s"

        try:
            self._execute_write(query, tuple(params))
            return True
        except Exception as e:
            logger.error("Error updating question %d: %s", question_id, str(e))
            return False

    def get_by_id(self, question_id: int) -> Optional[Dict[str, Any]]:
        """
        Get question by internal ID

        Args:
            question_id: Question ID

        Returns:
            Question data dictionary or None
        """
        query = "SELECT * FROM questions WHERE id = %s"
        results = self._execute_query(query, (question_id,))
        return results[0] if results else None

    def get_by_question_id(self, question_id: str) -> Optional[Dict[str, Any]]:
        """
        Get question by AWS re:Post question ID

        Args:
            question_id: AWS re:Post question ID

        Returns:
            Question data dictionary or None
        """
        query = "SELECT * FROM questions WHERE question_id = %s"
        results = self._execute_query(query, (question_id,))
        return results[0] if results else None

    def get_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get question by URL

        Args:
            url: Question URL

        Returns:
            Question data dictionary or None
        """
        query = "SELECT * FROM questions WHERE url = %s"
        results = self._execute_query(query, (url,))
        return results[0] if results else None

    def get_latest_questions(
        self, limit: int = 100, language: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get latest questions

        Args:
            limit: Number of questions to return
            language: Optional language filter ('en' or 'zh-Hant')

        Returns:
            List of question dictionaries
        """
        if language:
            query = """
            SELECT * FROM questions 
            WHERE language = %s
            ORDER BY posted_at DESC 
            LIMIT %s
            """
            params = (language, limit)
        else:
            query = "SELECT * FROM questions ORDER BY posted_at DESC LIMIT %s"
            params = (limit,)

        return self._execute_query(query, params) or []

    def get_questions_with_tags(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get questions with their tags

        Args:
            limit: Number of questions to return

        Returns:
            List of question dictionaries with tags
        """
        query = """
        SELECT 
            q.*,
            GROUP_CONCAT(t.name ORDER BY t.name SEPARATOR ', ') as tags
        FROM questions q
        LEFT JOIN question_tags qt ON q.id = qt.question_id
        LEFT JOIN tags t ON qt.tag_id = t.id
        GROUP BY q.id
        ORDER BY q.posted_at DESC
        LIMIT %s
        """
        return self._execute_query(query, (limit,)) or []

    def get_question_statistics(self, days: int = 30) -> Dict[str, Any]:
        """
        Get question statistics for the specified period

        Args:
            days: Number of days to analyze

        Returns:
            Statistics dictionary
        """
        query = """
        SELECT 
            COUNT(*) as total_questions,
            SUM(CASE WHEN language = 'en' THEN 1 ELSE 0 END) as english_count,
            SUM(CASE WHEN language = 'zh-Hant' THEN 1 ELSE 0 END) as chinese_count,
            SUM(has_accepted_answer) as accepted_answers,
            AVG(view_count) as avg_views,
            AVG(vote_count) as avg_votes,
            AVG(answers_count) as avg_answers,
            MAX(view_count) as max_views,
            MAX(vote_count) as max_votes
        FROM questions
        WHERE posted_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        results = self._execute_query(query, (days,))
        return results[0] if results else {}

    def search_questions(
        self, search_term: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search questions by title or description

        Args:
            search_term: Search term
            limit: Maximum number of results to return

        Returns:
            List of matching questions
        """
        query = """
        SELECT * FROM questions 
        WHERE title LIKE %s OR description LIKE %s
        ORDER BY posted_at DESC
        LIMIT %s
        """
        search_pattern = f"%{search_term}%"
        return self._execute_query(query, (search_pattern, search_pattern, limit)) or []

    def bulk_create_or_update(self, questions_data: List[Dict[str, Any]]) -> int:
        """
        Bulk create or update questions

        Args:
            questions_data: List of question data dictionaries

        Returns:
            Number of successfully processed questions
        """
        processed_count = 0
        for question_data in questions_data:
            try:
                if self.create_or_update(question_data):
                    processed_count += 1
            except Exception as e:
                logger.error(
                    "Error processing question %s: %s",
                    question_data.get("question_id"),
                    str(e),
                )
                continue
        return processed_count

    def delete_old_questions(self, days: int = 365) -> int:
        """
        Delete questions older than specified days

        Args:
            days: Age of questions to delete in days

        Returns:
            Number of questions deleted
        """
        query = """
        DELETE FROM questions 
        WHERE posted_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        try:
            return self._execute_write(query, (days,)) or 0
        except Exception as e:
            logger.error("Error deleting old questions: %s", str(e))
            return 0
