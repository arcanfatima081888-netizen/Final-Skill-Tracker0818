import sqlite3
from flask import Flask, render_template, request, flash, redirect, url_for, Response
from datetime import datetime, timedelta
import csv
from io import StringIO
import threading
import math
from contextlib import contextmanager

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

# Custom Exception Classes
class DatabaseError(Exception):
    """Base exception for database-related errors"""
    pass

class ValidationError(Exception):
    """Exception for validation errors"""
    pass

class SkillNotFoundError(Exception):
    """Exception when a skill is not found"""
    pass

# Database Manager Class
class DatabaseManager:
    _instance = None
    _thread_local = threading.local()
    
    def __new__(cls, db_path='skills.db'):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.db_path = db_path
            cls._instance.init_db()
        return cls._instance
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database operations"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise DatabaseError(f"Database operation failed: {str(e)}")
        finally:
            # Don't close the connection here as it's managed per thread
            pass
    
    def get_connection(self):
        """Get a database connection for the current thread"""
        if not hasattr(self._thread_local, 'db_connection'):
            try:
                self._thread_local.db_connection = sqlite3.connect(
                    self.db_path, check_same_thread=False
                )
                self._thread_local.db_connection.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                raise DatabaseError(f"Failed to connect to database: {str(e)}")
        return self._thread_local.db_connection
    
    def close_connection(self):
        """Close the database connection for the current thread"""
        if hasattr(self._thread_local, 'db_connection'):
            try:
                self._thread_local.db_connection.close()
            except sqlite3.Error as e:
                raise DatabaseError(f"Failed to close database connection: {str(e)}")
            finally:
                delattr(self._thread_local, 'db_connection')
    
    def init_db(self):
        """Initialize the database with required tables"""
        try:
            with self.get_cursor() as c:
                # Create skills table
                c.execute('''CREATE TABLE IF NOT EXISTS skills
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            category TEXT NOT NULL,
                            proficiency TEXT NOT NULL,
                            notes TEXT,
                            total_hours REAL DEFAULT 0,
                            last_practiced TIMESTAMP,
                            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                # Create practice_sessions table
                c.execute('''CREATE TABLE IF NOT EXISTS practice_sessions
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            skill_id INTEGER NOT NULL,
                            date TIMESTAMP NOT NULL,
                            duration REAL NOT NULL,
                            notes TEXT,
                            FOREIGN KEY (skill_id) REFERENCES skills (id))''')
                
                # Create streaks table
                c.execute('''CREATE TABLE IF NOT EXISTS streaks
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            skill_id INTEGER,
                            current_streak INTEGER DEFAULT 0,
                            longest_streak INTEGER DEFAULT 0,
                            last_practice_date TIMESTAMP,
                            FOREIGN KEY (skill_id) REFERENCES skills (id))''')
                
                # Create badges table
                c.execute('''CREATE TABLE IF NOT EXISTS badges
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            description TEXT NOT NULL,
                            criteria TEXT NOT NULL,
                            icon TEXT DEFAULT 'badge',
                            color TEXT DEFAULT '#4CAF50',
                            rarity TEXT DEFAULT 'common')''')
                
                # Create earned_badges table
                c.execute('''CREATE TABLE IF NOT EXISTS earned_badges
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            badge_id INTEGER NOT NULL,
                            skill_id INTEGER,
                            earned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (badge_id) REFERENCES badges (id),
                            FOREIGN KEY (skill_id) REFERENCES skills (id))''')
                
                # Insert default badges if they don't exist
                default_badges = [
                    ('First Steps', 'Logged your first practice session', 'first_practice', 'emoji_events', '#2196F3', 'common'),
                    ('Consistent Learner', 'Practiced for 7 consecutive days', '7_day_streak', 'local_fire_department', '#FF9800', 'uncommon'),
                    ('Dedicated', 'Practiced for 30 consecutive days', '30_day_streak', 'whatshot', '#F44336', 'rare'),
                    ('Marathoner', 'Practiced for 100 consecutive days', '100_day_streak', 'directions_run', '#9C27B0', 'epic'),
                    ('Hour Master', 'Logged 10 hours of practice', '10_hours', 'timer', '#4CAF50', 'common'),
                    ('Veteran', 'Logged 100 hours of practice', '100_hours', 'military_tech', '#607D8B', 'rare'),
                    ('Expert', 'Reached expert proficiency in a skill', 'expert_level', 'school', '#FFD700', 'epic'),
                    ('Jack of All Trades', 'Learned 5 different skills', '5_skills', 'diversity_3', '#00BCD4', 'uncommon'),
                    ('Master of Some', 'Learned 10 different skills', '10_skills', 'workspace_premium', '#E91E63', 'rare'),
                    ('Skill Collector', 'Learned 25 different skills', '25_skills', 'collections', '#795548', 'epic'),
                    ('Time Traveler', 'Logged practice sessions for 365 days', '365_days', 'history', '#3F51B5', 'legendary'),
                    ('Master Craftsman', 'Logged 1000 hours across all skills', '1000_hours', 'construction', '#FF5722', 'legendary')
                ]
                
                c.execute("SELECT COUNT(*) FROM badges")
                if c.fetchone()[0] == 0:
                    c.executemany("INSERT INTO badges (name, description, criteria, icon, color, rarity) VALUES (?, ?, ?, ?, ?, ?)", default_badges)
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize database: {str(e)}")

# Skill Model Class
class Skill:
    def __init__(self, id=None, name=None, category=None, proficiency=None, notes=None, 
                 total_hours=0, last_practiced=None, created_date=None):
        self.id = id
        self.name = name
        self.category = category
        self.proficiency = proficiency
        self.notes = notes
        self.total_hours = total_hours
        self.last_practiced = last_practiced
        self.created_date = created_date
    
    @classmethod
    def from_db_row(cls, row):
        """Create a Skill instance from a database row"""
        return cls(
            id=row['id'],
            name=row['name'],
            category=row['category'],
            proficiency=row['proficiency'],
            notes=row['notes'],
            total_hours=row['total_hours'],
            last_practiced=row['last_practiced'],
            created_date=row['created_date']
        )
    
    def save(self, db_manager):
        """Save the skill to the database"""
        try:
            with db_manager.get_cursor() as c:
                if self.id is None:
                    # Insert new skill
                    c.execute("INSERT INTO skills (name, category, proficiency, notes) VALUES (?, ?, ?, ?)",
                             (self.name, self.category, self.proficiency, self.notes))
                    self.id = c.lastrowid
                    
                    # Initialize streak for new skill
                    c.execute("INSERT INTO streaks (skill_id) VALUES (?)", (self.id,))
                else:
                    # Update existing skill
                    c.execute("UPDATE skills SET name=?, category=?, proficiency=?, notes=? WHERE id=?",
                             (self.name, self.category, self.proficiency, self.notes, self.id))
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to save skill: {str(e)}")
    
    def delete(self, db_manager):
        """Delete the skill from the database"""
        try:
            with db_manager.get_cursor() as c:
                c.execute("DELETE FROM practice_sessions WHERE skill_id=?", (self.id,))
                c.execute("DELETE FROM skills WHERE id=?", (self.id,))
                c.execute("DELETE FROM streaks WHERE skill_id=?", (self.id,))
                c.execute("DELETE FROM earned_badges WHERE skill_id=?", (self.id,))
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to delete skill: {str(e)}")
    
    def validate(self):
        """Validate the skill data"""
        if not self.name or not self.category or not self.proficiency:
            raise ValidationError("Please fill in all required fields")
        
        if is_numeric_string(self.name):
            raise ValidationError("Skill name must be text, not a number")
            
        if is_numeric_string(self.category):
            raise ValidationError("Category must be text, not a number")
            
        if is_numeric_string(self.proficiency):
            raise ValidationError("Proficiency must be text, not a number")
        
        return True

# Practice Session Model Class
class PracticeSession:
    def __init__(self, id=None, skill_id=None, date=None, duration=None, notes=None):
        self.id = id
        self.skill_id = skill_id
        self.date = date
        self.duration = duration
        self.notes = notes
    
    @classmethod
    def from_db_row(cls, row):
        """Create a PracticeSession instance from a database row"""
        return cls(
            id=row['id'],
            skill_id=row['skill_id'],
            date=row['date'],
            duration=row['duration'],
            notes=row['notes']
        )
    
    def save(self, db_manager):
        """Save the practice session to the database"""
        try:
            with db_manager.get_cursor() as c:
                c.execute("INSERT INTO practice_sessions (skill_id, date, duration, notes) VALUES (?, ?, ?, ?)",
                         (self.skill_id, self.date, self.duration, self.notes))
                
                c.execute("UPDATE skills SET total_hours = total_hours + ?, last_practiced = ? WHERE id = ?",
                         (self.duration, self.date, self.skill_id))
                
                # Update streak information
                self.update_streak(c)
                
                # Check for badge achievements
                self.check_badge_achievements(c)
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to save practice session: {str(e)}")
    
    def update_streak(self, cursor):
        """Update streak information for the skill"""
        try:
            # Get current streak info
            cursor.execute("SELECT current_streak, longest_streak, last_practice_date FROM streaks WHERE skill_id = ?", (self.skill_id,))
            streak_info = cursor.fetchone()
            
            if not streak_info:
                # Initialize streak if it doesn't exist
                cursor.execute("INSERT INTO streaks (skill_id, current_streak, longest_streak, last_practice_date) VALUES (?, 1, 1, ?)",
                             (self.skill_id, self.date))
                return
            
            current_streak, longest_streak, last_practice_date = streak_info
            
            if last_practice_date:
                last_date = datetime.strptime(last_practice_date, '%Y-%m-%d %H:%M:%S')
                practice_dt = datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
                
                # Check if practice was consecutive (within 36 hours of last practice)
                time_diff = practice_dt - last_date
                if time_diff <= timedelta(hours=36):
                    # Continue streak
                    new_streak = current_streak + 1
                    new_longest = max(new_streak, longest_streak)
                    cursor.execute("UPDATE streaks SET current_streak = ?, longest_streak = ?, last_practice_date = ? WHERE skill_id = ?",
                                 (new_streak, new_longest, self.date, self.skill_id))
                else:
                    # Reset streak
                    cursor.execute("UPDATE streaks SET current_streak = 1, last_practice_date = ? WHERE skill_id = ?",
                                 (self.date, self.skill_id))
            else:
                # First practice for this skill
                cursor.execute("UPDATE streaks SET current_streak = 1, longest_streak = 1, last_practice_date = ? WHERE skill_id = ?",
                             (self.date, self.skill_id))
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to update streak: {str(e)}")
    
    def check_badge_achievements(self, cursor):
        """Check for badge achievements after a practice session"""
        try:
            # Get skill information
            cursor.execute("SELECT total_hours, proficiency FROM skills WHERE id = ?", (self.skill_id,))
            skill_info = cursor.fetchone()
            total_hours = skill_info[0] if skill_info and skill_info[0] else 0
            proficiency = skill_info[1] if skill_info else ''
            
            # Get streak information
            cursor.execute("SELECT current_streak FROM streaks WHERE skill_id = ?", (self.skill_id,))
            streak_info = cursor.fetchone()
            current_streak = streak_info[0] if streak_info else 0
            
            # Get count of all skills
            cursor.execute("SELECT COUNT(*) FROM skills")
            skills_count = cursor.fetchone()[0]
            
            # Get count of practice sessions
            cursor.execute("SELECT COUNT(*) FROM practice_sessions WHERE skill_id = ?", (self.skill_id,))
            sessions_count = cursor.fetchone()[0]
            
            # Get total practice days
            cursor.execute("SELECT COUNT(DISTINCT DATE(date)) FROM practice_sessions")
            practice_days = cursor.fetchone()[0] or 0
            
            # Get total hours across all skills
            cursor.execute("SELECT SUM(total_hours) FROM skills")
            total_all_hours = cursor.fetchone()[0] or 0
            
            # Check for various badge criteria
            badges_to_check = [
                ('first_practice', sessions_count >= 1),
                ('7_day_streak', current_streak >= 7),
                ('30_day_streak', current_streak >= 30),
                ('100_day_streak', current_streak >= 100),
                ('10_hours', total_hours >= 10),
                ('100_hours', total_hours >= 100),
                ('expert_level', proficiency == 'Expert'),
                ('5_skills', skills_count >= 5),
                ('10_skills', skills_count >= 10),
                ('25_skills', skills_count >= 25),
                ('365_days', practice_days >= 365),
                ('1000_hours', total_all_hours >= 1000)
            ]
            
            for criteria, condition in badges_to_check:
                if condition:
                    # Check if badge already earned for this skill
                    cursor.execute('''SELECT eb.id FROM earned_badges eb 
                                   JOIN badges b ON eb.badge_id = b.id 
                                   WHERE b.criteria = ? AND (eb.skill_id = ? OR eb.skill_id IS NULL)''', 
                                   (criteria, self.skill_id))
                    if not cursor.fetchone():
                        # Award the badge
                        cursor.execute("SELECT id FROM badges WHERE criteria = ?", (criteria,))
                        badge_id_result = cursor.fetchone()
                        if badge_id_result:
                            badge_id = badge_id_result[0]
                            cursor.execute("INSERT INTO earned_badges (badge_id, skill_id) VALUES (?, ?)",
                                         (badge_id, self.skill_id if criteria not in ['5_skills', '10_skills', '25_skills', '365_days', '1000_hours'] else None))
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to check badge achievements: {str(e)}")
    
    def validate(self):
        """Validate the practice session data"""
        try:
            self.duration = float(self.duration)
            if self.duration <= 0:
                raise ValidationError("Duration must be a positive number")
        except ValueError:
            raise ValidationError("Duration must be a number")
        
        try:
            datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise ValidationError("Invalid date format")
        
        return True

# Service Classes for Business Logic
class SkillService:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_all_skills(self):
        """Get all skills from the database"""
        try:
            with self.db_manager.get_cursor() as c:
                c.execute("SELECT * FROM skills ORDER BY name")
                return [Skill.from_db_row(row) for row in c.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch skills: {str(e)}")
    
    def get_skill(self, skill_id):
        """Get a specific skill by ID"""
        try:
            with self.db_manager.get_cursor() as c:
                c.execute("SELECT * FROM skills WHERE id = ?", (skill_id,))
                row = c.fetchone()
                if not row:
                    raise SkillNotFoundError(f"Skill with ID {skill_id} not found")
                return Skill.from_db_row(row)
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch skill: {str(e)}")
    
    def create_skill(self, name, category, proficiency, notes):
        """Create a new skill"""
        skill = Skill(name=name, category=category, proficiency=proficiency, notes=notes)
        skill.validate()
        skill.save(self.db_manager)
        return skill
    
    def update_skill(self, skill_id, name, category, proficiency, notes):
        """Update an existing skill"""
        skill = self.get_skill(skill_id)
        skill.name = name
        skill.category = category
        skill.proficiency = proficiency
        skill.notes = notes
        skill.validate()
        skill.save(self.db_manager)
        return skill
    
    def delete_skill(self, skill_id):
        """Delete a skill"""
        skill = self.get_skill(skill_id)
        skill.delete(self.db_manager)

class PracticeService:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_practice_sessions(self, skill_id):
        """Get all practice sessions for a skill"""
        try:
            with self.db_manager.get_cursor() as c:
                c.execute("SELECT * FROM practice_sessions WHERE skill_id = ? ORDER BY date DESC", (skill_id,))
                return [PracticeSession.from_db_row(row) for row in c.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch practice sessions: {str(e)}")
    
    def get_recent_practice_sessions(self, limit=10):
        """Get recent practice sessions across all skills"""
        try:
            with self.db_manager.get_cursor() as c:
                c.execute("SELECT ps.*, s.name as skill_name FROM practice_sessions ps " +
                         "JOIN skills s ON ps.skill_id = s.id ORDER BY ps.date DESC LIMIT ?", (limit,))
                return c.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch recent practice sessions: {str(e)}")
    
    def add_practice_session(self, skill_id, date, duration, notes):
        """Add a new practice session"""
        session = PracticeSession(skill_id=skill_id, date=date, duration=duration, notes=notes)
        session.validate()
        session.save(self.db_manager)
        return session

class ReportService:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def generate_progress_report(self):
        """Generate a comprehensive progress report"""
        try:
            with self.db_manager.get_cursor() as c:
                # Get all skills with practice information
                c.execute('''SELECT s.name, s.category, s.proficiency, s.total_hours, 
                            COUNT(ps.id) as session_count, 
                            MAX(ps.date) as last_practiced,
                            st.current_streak, st.longest_streak
                            FROM skills s
                            LEFT JOIN practice_sessions ps ON s.id = ps.skill_id
                            LEFT JOIN streaks st ON s.id = st.skill_id
                            GROUP BY s.id
                            ORDER BY s.category, s.name''')
                
                skills = c.fetchall()
                
                # Get total statistics
                c.execute("SELECT COUNT(*), SUM(total_hours) FROM skills")
                total_result = c.fetchone()
                total_skills = total_result[0] if total_result else 0
                total_hours = total_result[1] if total_result and total_result[1] else 0
                
                c.execute("SELECT COUNT(*) FROM practice_sessions")
                total_sessions_result = c.fetchone()
                total_sessions = total_sessions_result[0] if total_sessions_result else 0
                
                # Get streak statistics
                c.execute("SELECT AVG(current_streak), MAX(current_streak), AVG(longest_streak), MAX(longest_streak) FROM streaks")
                streak_stats = c.fetchone()
                avg_streak = streak_stats[0] or 0
                max_streak = streak_stats[1] or 0
                avg_longest_streak = streak_stats[2] or 0
                max_longest_streak = streak_stats[3] or 0
                
                # Get badge statistics
                c.execute("SELECT COUNT(*) FROM earned_badges")
                total_badges = c.fetchone()[0] or 0
                
                c.execute("SELECT COUNT(DISTINCT badge_id) FROM earned_badges")
                unique_badges = c.fetchone()[0] or 0
                
                # Get practice frequency
                c.execute("SELECT COUNT(DISTINCT DATE(date)) FROM practice_sessions")
                practice_days = c.fetchone()[0] or 0
                
                # Get category distribution
                c.execute("SELECT category, COUNT(*) as count, SUM(total_hours) as hours FROM skills GROUP BY category ORDER BY hours DESC")
                categories = c.fetchall()
                
                # Get proficiency distribution
                c.execute("SELECT proficiency, COUNT(*) as count, SUM(total_hours) as hours FROM skills GROUP BY proficiency ORDER BY hours DESC")
                proficiencies = c.fetchall()
                
                # Get recent activity
                c.execute("SELECT DATE(date) as practice_date, SUM(duration) as total_hours, COUNT(*) as sessions FROM practice_sessions WHERE date >= date('now', '-30 days') GROUP BY DATE(date) ORDER BY practice_date DESC")
                recent_activity = c.fetchall()
                
                # Get earned badges
                c.execute('''SELECT b.id, b.name, b.description, b.icon, b.color, b.rarity,
                            eb.earned_date, s.name as skill_name
                         FROM earned_badges eb
                         JOIN badges b ON eb.badge_id = b.id
                         LEFT JOIN skills s ON eb.skill_id = s.id
                         ORDER BY eb.earned_date DESC''')
                badges = c.fetchall()
                
                # Create CSV content
                output = StringIO()
                writer = csv.writer(output)
                
                # Write header
                writer.writerow(['SKILL TRACKER - COMPREHENSIVE PROGRESS REPORT'])
                writer.writerow(['Generated on', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                writer.writerow(['Report Period', 'All Time'])
                writer.writerow([])
                
                # Overall Statistics
                writer.writerow(['OVERALL STATISTICS'])
                writer.writerow(['Total Skills', total_skills])
                writer.writerow(['Total Practice Hours', round(total_hours or 0, 2)])
                writer.writerow(['Total Practice Sessions', total_sessions])
                writer.writerow(['Total Practice Days', practice_days])
                writer.writerow(['Average Daily Practice (hours)', round(total_hours/max(practice_days, 1), 2) if practice_days > 0 else 0])
                writer.writerow(['Badges Earned', f"{total_badges} ({unique_badges} unique)"])
                writer.writerow(['Current Streak (avg/max)', f"{round(avg_streak, 1)} / {max_streak}"])
                writer.writerow(['Longest Streak (avg/max)', f"{round(avg_longest_streak, 1)} / {max_longest_streak}"])
                writer.writerow([])
                
                # Category Distribution
                writer.writerow(['CATEGORY DISTRIBUTION'])
                writer.writerow(['Category', 'Skills', 'Hours', 'Percentage'])
                for category in categories:
                    category_name, skill_count, hours = category['category'], category['count'], category['hours'] or 0
                    percentage = (hours / total_hours * 100) if total_hours > 0 else 0
                    writer.writerow([category_name, skill_count, round(hours, 2), f"{round(percentage, 1)}%"])
                writer.writerow([])
                
                # Proficiency Distribution
                writer.writerow(['PROFICIENCY DISTRIBUTION'])
                writer.writerow(['Proficiency', 'Skills', 'Hours', 'Percentage'])
                for proficiency in proficiencies:
                    prof_name, skill_count, hours = proficiency['proficiency'], proficiency['count'], proficiency['hours'] or 0
                    percentage = (hours / total_hours * 100) if total_hours > 0 else 0
                    writer.writerow([prof_name, skill_count, round(hours, 2), f"{round(percentage, 1)}%"])
                writer.writerow([])
                
                # Skills Summary
                writer.writerow(['SKILLS SUMMARY'])
                writer.writerow(['Name', 'Category', 'Proficiency', 'Total Hours', 'Sessions', 'Avg Session', 'Current Streak', 'Longest Streak', 'Last Practiced', 'Days Since Practice'])
                
                today = datetime.now().date()
                for skill in skills:
                    name, category, proficiency, total_hours, session_count, last_practiced, current_streak, longest_streak = skill
                    avg_session = round(total_hours / session_count, 2) if session_count > 0 else 0
                    
                    # Calculate days since last practice
                    days_since = "N/A"
                    if last_practiced:
                        last_date = datetime.strptime(last_practiced, '%Y-%m-%d %H:%M:%S').date()
                        days_since = (today - last_date).days
                    
                    writer.writerow([
                        name, 
                        category, 
                        proficiency, 
                        round(total_hours or 0, 2), 
                        session_count or 0,
                        avg_session,
                        current_streak or 0,
                        longest_streak or 0,
                        last_practiced or 'Never',
                        days_since
                    ])
                
                # Recent Activity (last 30 days)
                writer.writerow([])
                writer.writerow(['RECENT ACTIVITY (LAST 30 DAYS)'])
                writer.writerow(['Date', 'Total Hours', 'Sessions'])
                for activity in recent_activity:
                    writer.writerow([
                        activity['practice_date'],
                        round(activity['total_hours'] or 0, 2),
                        activity['sessions'] or 0
                    ])
                
                # Badges Summary
                writer.writerow([])
                writer.writerow(['BADGES EARNED'])
                writer.writerow(['Badge Name', 'Description', 'Earned Date', 'Related Skill', 'Rarity'])
                for badge in badges:
                    writer.writerow([
                        badge['name'],
                        badge['description'],
                        badge['earned_date'],
                        badge['skill_name'] or 'General',
                        badge['rarity'].capitalize()
                    ])
                
                output.seek(0)
                return output.getvalue()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to generate progress report: {str(e)}")

class BadgeService:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_earned_badges(self, skill_id=None):
        """Get earned badges, optionally filtered by skill"""
        try:
            with self.db_manager.get_cursor() as c:
                if skill_id:
                    c.execute('''SELECT b.id, b.name, b.description, b.icon, b.color, b.rarity, 
                              eb.earned_date, s.name as skill_name
                           FROM earned_badges eb
                           JOIN badges b ON eb.badge_id = b.id
                           LEFT JOIN skills s ON eb.skill_id = s.id
                           WHERE eb.skill_id = ? OR eb.skill_id IS NULL
                           ORDER BY eb.earned_date DESC''', (skill_id,))
                else:
                    c.execute('''SELECT b.id, b.name, b.description, b.icon, b.color, b.rarity,
                              eb.earned_date, s.name as skill_name
                           FROM earned_badges eb
                           JOIN badges b ON eb.badge_id = b.id
                           LEFT JOIN skills s ON eb.skill_id = s.id
                           ORDER BY eb.earned_date DESC''')
                
                return c.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch earned badges: {str(e)}")
    
    def get_all_badges(self):
        """Get all available badges, including unearned ones"""
        try:
            with self.db_manager.get_cursor() as c:
                c.execute('''SELECT b.*, 
                         CASE WHEN eb.id IS NOT NULL THEN 1 ELSE 0 END as earned,
                         eb.earned_date,
                         s.name as skill_name
                         FROM badges b
                         LEFT JOIN earned_badges eb ON b.id = eb.badge_id
                         LEFT JOIN skills s ON eb.skill_id = s.id
                         ORDER BY b.rarity DESC, b.name''')
                
                return c.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch all badges: {str(e)}")

class StreakService:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_streak_info(self, skill_id=None):
        """Get streak information, optionally filtered by skill"""
        try:
            with self.db_manager.get_cursor() as c:
                if skill_id:
                    c.execute('''SELECT s.name, st.current_streak, st.longest_streak, st.last_practice_date
                              FROM streaks st
                              JOIN skills s ON st.skill_id = s.id
                              WHERE st.skill_id = ?''', (skill_id,))
                else:
                    c.execute('''SELECT s.name, st.current_streak, st.longest_streak, st.last_practice_date
                              FROM streaks st
                              JOIN skills s ON st.skill_id = s.id
                              ORDER BY st.current_streak DESC''')
                
                return c.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to fetch streak information: {str(e)}")

# Utility functions
def is_numeric_string(value):
    """Check if a string can be converted to a number (int or float)"""
    try:
        float(value)
        return True
    except ValueError:
        return False

def get_proficiency_counts(db_manager):
    """Get counts of skills by proficiency level"""
    try:
        with db_manager.get_cursor() as c:
            c.execute("SELECT proficiency, COUNT(*) FROM skills GROUP BY proficiency")
            counts = dict(c.fetchall())
            
            all_levels = ['Beginner', 'Intermediate', 'Advanced', 'Expert']
            return {level: counts.get(level, 0) for level in all_levels}
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to fetch proficiency counts: {str(e)}")

def get_skill_practice_summary(db_manager, days=30):
    """Get practice summary for all skills over a period"""
    try:
        with db_manager.get_cursor() as c:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            c.execute("SELECT s.id, s.name, s.category, SUM(ps.duration) as total_hours, " +
                     "COUNT(ps.id) as session_count " +
                     "FROM skills s LEFT JOIN practice_sessions ps ON s.id = ps.skill_id " +
                     "AND ps.date >= ? " +
                     "GROUP BY s.id ORDER by total_hours DESC", (cutoff_date,))
            return c.fetchall()
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to fetch skill practice summary: {str(e)}")

# Initialize services
db_manager = DatabaseManager()
skill_service = SkillService(db_manager)
practice_service = PracticeService(db_manager)
report_service = ReportService(db_manager)
badge_service = BadgeService(db_manager)
streak_service = StreakService(db_manager)

# Register the close_db_connection function to be called when the app context tears down
@app.teardown_appcontext
def close_db_connection(e=None):
    """Close the database connection at the end of the request"""
    db_manager.close_connection()

# Routes
@app.route('/')
def index():
    try:
        skills = skill_service.get_all_skills()
        recent_sessions = practice_service.get_recent_practice_sessions(5)
        return render_template('index.html', skills=skills, recent_sessions=recent_sessions)
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('index.html', skills=[], recent_sessions=[])

@app.route('/add_skill', methods=['GET', 'POST'])
def add_skill_page():
    if request.method == 'POST':
        try:
            name = request.form['name']
            category = request.form['category']
            proficiency = request.form['proficiency']
            notes = request.form.get('notes', '')
            
            skill_service.create_skill(name, category, proficiency, notes)
            flash('Skill added successfully!', 'success')
            return redirect(url_for('view_skills'))
            
        except ValidationError as e:
            flash(str(e), 'error')
            return render_template('add_skill.html')
        except DatabaseError as e:
            flash(f'Database error: {str(e)}', 'error')
            return render_template('add_skill.html')
    
    return render_template('add_skill.html')

@app.route('/skills')
def view_skills():
    try:
        skills = skill_service.get_all_skills()
        return render_template('view_skills.html', skills=skills)
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('view_skills.html', skills=[])

@app.route('/skill/<int:skill_id>')
def skill_detail(skill_id):
    try:
        skill = skill_service.get_skill(skill_id)
        sessions = practice_service.get_practice_sessions(skill_id)
        badges = badge_service.get_earned_badges(skill_id)
        streak_info = streak_service.get_streak_info(skill_id)
        
        return render_template('skill_detail.html', skill=skill, sessions=sessions, badges=badges, streak_info=streak_info)
    except SkillNotFoundError as e:
        flash(str(e), 'error')
        return redirect(url_for('view_skills'))
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return redirect(url_for('view_skills'))

@app.route('/update_skill/<int:skill_id>', methods=['GET', 'POST'])
def update_skill_page(skill_id):
    try:
        skill = skill_service.get_skill(skill_id)
        
        if request.method == 'POST':
            try:
                name = request.form['name']
                category = request.form['category']
                proficiency = request.form['proficiency']
                notes = request.form.get('notes', '')
                
                skill_service.update_skill(skill_id, name, category, proficiency, notes)
                flash('Skill updated successfully!', 'success')
                return redirect(url_for('view_skills'))
                
            except ValidationError as e:
                flash(str(e), 'error')
                return render_template('update_skill.html', skill=skill)
            except DatabaseError as e:
                flash(f'Database error: {str(e)}', 'error')
                return render_template('update_skill.html', skill=skill)
        
        return render_template('update_skill.html', skill=skill)
    
    except SkillNotFoundError as e:
        flash(str(e), 'error')
        return redirect(url_for('view_skills'))
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return redirect(url_for('view_skills'))

@app.route('/delete_skill/<int:skill_id>')
def delete_skill_page(skill_id):
    try:
        skill_service.delete_skill(skill_id)
        flash('Skill deleted successfully!', 'success')
    except SkillNotFoundError as e:
        flash(str(e), 'error')
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
    
    return redirect(url_for('view_skills'))

@app.route('/add_practice/<int:skill_id>', methods=['GET', 'POST'])
def add_practice(skill_id):
    try:
        skill = skill_service.get_skill(skill_id)
        
        if request.method == 'POST':
            try:
                date_str = request.form['date']
                duration_str = request.form['duration']
                notes = request.form.get('notes', '')
                
                # Convert date to the format expected by the database
                practice_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
                formatted_date = practice_date.strftime('%Y-%m-%d %H:%M:%S')
                
                practice_service.add_practice_session(skill_id, formatted_date, duration_str, notes)
                flash('Practice session logged successfully!', 'success')
                return redirect(url_for('skill_detail', skill_id=skill_id))
                
            except ValidationError as e:
                flash(str(e), 'error')
                default_date = datetime.now().strftime('%Y-%m-%dT%H:%M')
                return render_template('add_practice.html', skill=skill, default_date=default_date)
            except DatabaseError as e:
                flash(f'Database error: {str(e)}', 'error')
                default_date = datetime.now().strftime('%Y-%m-%dT%H:%M')
                return render_template('add_practice.html', skill=skill, default_date=default_date)
        
        default_date = datetime.now().strftime('%Y-%m-%dT%H:%M')
        return render_template('add_practice.html', skill=skill, default_date=default_date)
    
    except SkillNotFoundError as e:
        flash(str(e), 'error')
        return redirect(url_for('view_skills'))
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return redirect(url_for('view_skills'))

@app.route('/practice_history')
def practice_history():
    try:
        sessions = practice_service.get_recent_practice_sessions(50)
        return render_template('practice_history.html', sessions=sessions)
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('practice_history.html', sessions=[])

@app.route('/dashboard')
def dashboard():
    try:
        skills = skill_service.get_all_skills()
        proficiency_counts = get_proficiency_counts(db_manager)
        monthly_summary = get_skill_practice_summary(db_manager, 30)
        
        total_hours = sum(skill.total_hours or 0 for skill in skills)
        total_skills = len(skills)
        
        proficiency_definitions = {
            'Beginner': 'Basic understanding, can perform tasks with guidance',
            'Intermediate': 'Good understanding, can perform tasks independently',
            'Advanced': 'Deep understanding, can solve complex problems',
            'Expert': 'Mastery, can teach others and develop new approaches'
        }
        
        return render_template('dashboard.html', 
                             proficiency_counts=proficiency_counts,
                             total_skills=total_skills,
                             total_hours=total_hours,
                             monthly_summary=monthly_summary,
                             proficiency_definitions=proficiency_definitions)
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('dashboard.html', 
                             proficiency_counts={},
                             total_skills=0,
                             total_hours=0,
                             monthly_summary=[],
                             proficiency_definitions={})

@app.route('/progress_report')
def progress_report():
    try:
        report = report_service.generate_progress_report()
        return Response(
            report,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=skill_progress_report.csv"}
        )
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/achievements')
def achievements():
    try:
        badges = badge_service.get_earned_badges()
        all_badges = badge_service.get_all_badges()
        streaks = streak_service.get_streak_info()
        
        # Calculate statistics for the dashboard
        total_badges = len(badges)
        unique_badges = len(set(badge['id'] for badge in badges))
        
        # Group badges by rarity
        rarity_counts = {}
        for badge in badges:
            rarity = badge['rarity']
            rarity_counts[rarity] = rarity_counts.get(rarity, 0) + 1
        
        # Get top streaks
        top_streaks = sorted([(s['name'], s['current_streak']) for s in streaks], key=lambda x: x[1], reverse=True)[:5]
        
        return render_template('achievements.html', 
                             badges=badges,
                             all_badges=all_badges,
                             streaks=streaks,
                             total_badges=total_badges,
                             unique_badges=unique_badges,
                             rarity_counts=rarity_counts,
                             top_streaks=top_streaks)
    except DatabaseError as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('achievements.html', 
                             badges=[],
                             all_badges=[],
                             streaks=[],
                             total_badges=0,
                             unique_badges=0,
                             rarity_counts={},
                             top_streaks=[])

if __name__ == '__main__':
     app.run(debug=True, host='0.0.0.0', port=5000)