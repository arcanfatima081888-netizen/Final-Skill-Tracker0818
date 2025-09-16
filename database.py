import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional
from models import Skill, PracticeSession

class DatabaseManager:
    def __init__(self, db_name='skills.db'):
        self.db_name = db_name
        self.init_db()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self):
        with self.get_connection() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS skills
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         name TEXT NOT NULL,
                         category TEXT NOT NULL,
                         proficiency TEXT NOT NULL,
                         notes TEXT,
                         total_hours REAL DEFAULT 0,
                         last_practiced TIMESTAMP,
                         created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            
            conn.execute('''CREATE TABLE IF NOT EXISTS practice_sessions
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         skill_id INTEGER NOT NULL,
                         date TIMESTAMP NOT NULL,
                         duration REAL NOT NULL,
                         notes TEXT,
                         FOREIGN KEY (skill_id) REFERENCES skills (id))''')
            conn.commit()
    
    def add_skill(self, name: str, category: str, proficiency: str, notes: str = "") -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO skills (name, category, proficiency, notes) VALUES (?, ?, ?, ?)",
                (name, category, proficiency, notes)
            )
            conn.commit()
            return cursor.lastrowid
    
    def get_all_skills(self) -> List[Skill]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM skills ORDER BY name")
            return [Skill(**dict(row)) for row in cursor.fetchall()]
    
    def get_skill(self, skill_id: int) -> Optional[Skill]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM skills WHERE id = ?", (skill_id,))
            row = cursor.fetchone()
            return Skill(**dict(row)) if row else None
    
    def update_skill(self, skill_id: int, name: str, category: str, proficiency: str, notes: str):
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE skills SET name=?, category=?, proficiency=?, notes=? WHERE id=?",
                (name, category, proficiency, notes, skill_id)
            )
            conn.commit()
    
    def delete_skill(self, skill_id: int):
        with self.get_connection() as conn:
            conn.execute("DELETE FROM practice_sessions WHERE skill_id=?", (skill_id,))
            conn.execute("DELETE FROM skills WHERE id=?", (skill_id,))
            conn.commit()
    
    def add_practice_session(self, skill_id: int, date: datetime, duration: float, notes: str = "") -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO practice_sessions (skill_id, date, duration, notes) VALUES (?, ?, ?, ?)",
                (skill_id, date, duration, notes)
            )
            
            conn.execute(
                "UPDATE skills SET total_hours = total_hours + ?, last_practiced = ? WHERE id = ?",
                (duration, date, skill_id)
            )
            
            conn.commit()
            return cursor.lastrowid
    
    def get_practice_sessions(self, skill_id: int) -> List[PracticeSession]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM practice_sessions WHERE skill_id = ? ORDER BY date DESC",
                (skill_id,)
            )
            return [PracticeSession(**dict(row)) for row in cursor.fetchall()]
    
    def get_recent_practice_sessions(self, limit: int = 10) -> List[PracticeSession]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT ps.*, s.name as skill_name FROM practice_sessions ps " +
                "JOIN skills s ON ps.skill_id = s.id ORDER BY ps.date DESC LIMIT ?",
                (limit,)
            )
            sessions = []
            for row in cursor.fetchall():
                session = PracticeSession(**dict(row))
                session.skill_name = row['skill_name']
                sessions.append(session)
            return sessions
    
    def get_skill_practice_summary(self, days: int = 30) -> List[dict]:
        with self.get_connection() as conn:
            cutoff_date = datetime.now() - timedelta(days=days)
            cursor = conn.execute(
                "SELECT s.id, s.name, s.category, SUM(ps.duration) as total_hours, " +
                "COUNT(ps.id) as session_count " +
                "FROM skills s LEFT JOIN practice_sessions ps ON s.id = ps.skill_id " +
                "AND ps.date >= ? " +
                "GROUP BY s.id ORDER BY total_hours DESC",
                (cutoff_date.strftime('%Y-%m-%d %H:%M:%S'),)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_least_practiced_skills(self, limit: int = 5) -> List[Skill]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM skills WHERE last_practiced IS NULL OR " +
                "last_practiced < datetime('now', '-7 days') ORDER BY last_practiced LIMIT ?",
                (limit,)
            )
            return [Skill(**dict(row)) for row in cursor.fetchall()]