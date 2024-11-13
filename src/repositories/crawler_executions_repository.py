# repositories/crawler_executions_repository.py
import logging
from typing import Any, Dict, List, Optional

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CrawlerExecutionsRepository(BaseRepository):
    def create(self, execution_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new crawler execution record

        Args:
            execution_data: Dictionary containing:
                - start_time: Execution start time
                - end_time: Execution end time
                - questions_processed: Number of questions processed
                - english_questions: Number of English questions
                - chinese_questions: Number of Chinese questions
                - status: 'success' or 'error'
                - error_message: Error message (optional)
                - duration_ms: Execution duration in milliseconds
                - output_file: S3 output file path

        Returns:
            ID of the new execution record
        """
        query = """
        INSERT INTO crawler_executions (
            start_time, end_time, questions_processed,
            english_questions, chinese_questions, status,
            error_message, duration_ms, output_file
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params = (
            execution_data["start_time"],
            execution_data["end_time"],
            execution_data["questions_processed"],
            execution_data["english_questions"],
            execution_data["chinese_questions"],
            execution_data["status"],
            execution_data.get("error_message"),
            execution_data["duration_ms"],
            execution_data["output_file"],
        )

        return self._execute_write(query, params)

    def get_execution_by_id(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Get execution record by ID

        Args:
            execution_id: Execution record ID

        Returns:
            Execution record dictionary or None
        """
        query = "SELECT * FROM crawler_executions WHERE id = %s"
        results = self._execute_query(query, (execution_id,))
        return results[0] if results else None

    def get_latest_execution(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recent crawler execution

        Returns:
            Latest execution record or None
        """
        query = """
        SELECT * 
        FROM crawler_executions 
        ORDER BY start_time DESC 
        LIMIT 1
        """
        results = self._execute_query(query)
        return results[0] if results else None

    def get_daily_statistics(self, limit: int = 30) -> List[Dict[str, Any]]:
        """
        Get daily crawler execution statistics

        Args:
            limit: Number of days to return

        Returns:
            List of daily statistics
        """
        query = """
        SELECT 
            DATE(start_time) as crawl_date,
            COUNT(*) as total_executions,
            SUM(questions_processed) as total_questions,
            SUM(english_questions) as total_english,
            SUM(chinese_questions) as total_chinese,
            AVG(duration_ms) as avg_duration_ms,
            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count
        FROM crawler_executions
        GROUP BY DATE(start_time)
        ORDER BY crawl_date DESC
        LIMIT %s
        """
        return self._execute_query(query, (limit,)) or []

    def get_recent_executions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent crawler executions

        Args:
            limit: Number of executions to return

        Returns:
            List of execution records
        """
        query = """
        SELECT * 
        FROM crawler_executions 
        ORDER BY start_time DESC 
        LIMIT %s
        """
        return self._execute_query(query, (limit,)) or []

    def get_failed_executions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent failed executions

        Args:
            limit: Number of failed executions to return

        Returns:
            List of failed execution records
        """
        query = """
        SELECT * 
        FROM crawler_executions 
        WHERE status = 'error'
        ORDER BY start_time DESC 
        LIMIT %s
        """
        return self._execute_query(query, (limit,)) or []

    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Get overall execution statistics

        Returns:
            Dictionary containing summary statistics
        """
        query = """
        SELECT 
            COUNT(*) as total_executions,
            SUM(questions_processed) as total_questions_processed,
            AVG(duration_ms) as avg_duration_ms,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            COUNT(CASE WHEN status = 'error' THEN 1 END) as total_errors,
            SUM(english_questions) as total_english,
            SUM(chinese_questions) as total_chinese
        FROM crawler_executions
        """
        results = self._execute_query(query)
        return results[0] if results else {}

    def update_execution_status(
        self, execution_id: int, status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        Update the status of an execution

        Args:
            execution_id: Execution record ID
            status: New status ('success' or 'error')
            error_message: Optional error message

        Returns:
            True if successful
        """
        query = """
        UPDATE crawler_executions 
        SET status = %s, error_message = %s
        WHERE id = %s
        """
        try:
            self._execute_write(query, (status, error_message, execution_id))
            return True
        except Exception as e:
            logger.error("Error updating execution status: %s", str(e))
            return False

    def cleanup_old_records(self, days_to_keep: int = 90) -> int:
        """
        Delete execution records older than specified days

        Args:
            days_to_keep: Number of days of records to keep

        Returns:
            Number of records deleted
        """
        query = """
        DELETE FROM crawler_executions
        WHERE start_time < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        try:
            return self._execute_write(query, (days_to_keep,)) or 0
        except Exception as e:
            logger.error("Error cleaning up old records: %s", str(e))
            return 0
