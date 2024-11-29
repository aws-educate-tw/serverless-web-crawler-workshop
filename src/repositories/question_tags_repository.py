# repositories/question_tags_repository.py
import logging
from typing import Any, Dict, List, Set

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class QuestionTagsRepository(BaseRepository):
    def add_tags_to_question(self, question_id: int, tag_ids: List[int]) -> bool:
        """
        Add multiple tags to a question

        Args:
            question_id: Question ID
            tag_ids: List of tag IDs

        Returns:
            True if successful
        """
        if not tag_ids:
            return True

        query = "INSERT IGNORE INTO question_tags (question_id, tag_id) VALUES (%s, %s)"
        params = [(question_id, tag_id) for tag_id in tag_ids]

        try:
            self._execute_many(query, params)
            return True
        except Exception as e:
            logger.error("Error adding tags to question %d: %s", question_id, str(e))
            return False

    def remove_tags_from_question(self, question_id: int, tag_ids: List[int]) -> bool:
        """
        Remove specific tags from a question

        Args:
            question_id: Question ID
            tag_ids: List of tag IDs to remove

        Returns:
            True if successful
        """
        if not tag_ids:
            return True

        query = "DELETE FROM question_tags WHERE question_id = %s AND tag_id IN ({})".format(
            ",".join(["%s"] * len(tag_ids))
        )
        params = [question_id] + tag_ids

        try:
            self._execute_write(query, tuple(params))
            return True
        except Exception as e:
            logger.error(
                "Error removing tags from question %d: %s", question_id, str(e)
            )
            return False

    def get_question_tags(self, question_id: int) -> List[Dict[str, Any]]:
        """
        Get all tags for a specific question

        Args:
            question_id: Question ID

        Returns:
            List of tag dictionaries
        """
        query = """
        SELECT t.*
        FROM tags t
        JOIN question_tags qt ON t.id = qt.tag_id
        WHERE qt.question_id = %s
        ORDER BY t.name
        """
        return self._execute_query(query, (question_id,)) or []

    def get_questions_by_tag(
        self, tag_id: int, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get questions that have a specific tag

        Args:
            tag_id: Tag ID
            limit: Maximum number of questions to return

        Returns:
            List of question dictionaries
        """
        query = """
        SELECT q.*
        FROM questions q
        JOIN question_tags qt ON q.id = qt.question_id
        WHERE qt.tag_id = %s
        ORDER BY q.posted_at DESC
        LIMIT %s
        """
        return self._execute_query(query, (tag_id, limit)) or []

    def update_question_tags(self, question_id: int, tag_ids: Set[int]) -> bool:
        """
        Update the tags for a question (remove old ones, add new ones)

        Args:
            question_id: Question ID
            tag_ids: Set of tag IDs that should be associated with the question

        Returns:
            True if successful
        """
        try:
            # Get current tags
            query = "SELECT tag_id FROM question_tags WHERE question_id = %s"
            current_tags = self._execute_query(query, (question_id,)) or []
            current_tag_ids = {row["tag_id"] for row in current_tags}

            # Calculate differences
            tags_to_add = tag_ids - current_tag_ids
            tags_to_remove = current_tag_ids - tag_ids

            # Remove old tags
            if tags_to_remove:
                self.remove_tags_from_question(question_id, list(tags_to_remove))

            # Add new tags
            if tags_to_add:
                self.add_tags_to_question(question_id, list(tags_to_add))

            return True

        except Exception as e:
            logger.error("Error updating tags for question %d: %s", question_id, str(e))
            return False

    def get_tag_usage_counts(self) -> List[Dict[str, Any]]:
        """
        Get usage count for all tags

        Returns:
            List of dictionaries with tag usage statistics
        """
        query = """
        SELECT
            t.id,
            t.name,
            COUNT(qt.question_id) as question_count
        FROM tags t
        LEFT JOIN question_tags qt ON t.id = qt.tag_id
        GROUP BY t.id, t.name
        ORDER BY question_count DESC
        """
        return self._execute_query(query) or []
