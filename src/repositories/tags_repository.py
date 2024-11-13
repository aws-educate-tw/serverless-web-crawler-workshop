# repositories/tags_repository.py
import logging
from typing import Any, Dict, List, Optional

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class TagsRepository(BaseRepository):
    def create_or_get(self, name: str) -> Optional[int]:
        """
        Create a new tag or get existing tag ID

        Args:
            name: Tag name

        Returns:
            Tag ID
        """
        existing_tag = self.get_by_name(name)
        if existing_tag:
            return existing_tag["id"]

        return self.create({"name": name})

    def create(self, tag_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new tag

        Args:
            tag_data: Tag data dictionary

        Returns:
            New tag ID
        """
        query = "INSERT INTO tags (name) VALUES (%s)"
        return self._execute_write(query, (tag_data["name"],))

    def get_by_id(self, tag_id: int) -> Optional[Dict[str, Any]]:
        """
        Get tag by ID

        Args:
            tag_id: Tag ID

        Returns:
            Tag data dictionary or None
        """
        query = "SELECT * FROM tags WHERE id = %s"
        results = self._execute_query(query, (tag_id,))
        return results[0] if results else None

    def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get tag by name

        Args:
            name: Tag name

        Returns:
            Tag data dictionary or None
        """
        query = "SELECT * FROM tags WHERE name = %s"
        results = self._execute_query(query, (name,))
        return results[0] if results else None

    def list_all(self) -> List[Dict[str, Any]]:
        """
        Get all tags

        Returns:
            List of tag dictionaries
        """
        query = "SELECT * FROM tags ORDER BY name"
        return self._execute_query(query) or []

    def get_tag_statistics(self) -> List[Dict[str, Any]]:
        """
        Get usage statistics for all tags

        Returns:
            List of tag statistics
        """
        query = """
        SELECT
            t.id,
            t.name,
            COUNT(qt.question_id) as usage_count,
            COUNT(CASE WHEN q.language = 'en' THEN 1 END) as english_count,
            COUNT(CASE WHEN q.language = 'zh-Hant' THEN 1 END) as chinese_count
        FROM tags t
        LEFT JOIN question_tags qt ON t.id = qt.tag_id
        LEFT JOIN questions q ON qt.question_id = q.id
        GROUP BY t.id, t.name
        ORDER BY usage_count DESC
        """
        return self._execute_query(query) or []

    def bulk_create_or_get(self, names: List[str]) -> Dict[str, int]:
        """
        Bulk create or get tags by names

        Args:
            names: List of tag names

        Returns:
            Dictionary mapping tag names to their IDs
        """
        result = {}
        for name in names:
            try:
                result[name] = self.create_or_get(name)
            except Exception as e:
                logger.error("Error processing tag %s: %s", name, str(e))
                continue
        return result
