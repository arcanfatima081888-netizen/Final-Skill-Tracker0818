from datetime import datetime, timedelta
from typing import Dict, List
from models import PracticeSession

class ProgressUtils:
    @staticmethod
    def calculate_streak(sessions: List[PracticeSession]) -> int:
        if not sessions:
            return 0
            
        sorted_sessions = sorted(sessions, key=lambda x: x.date, reverse=True)
        
        streak = 0
        current_date = datetime.now().date()
        
        for session in sorted_sessions:
            session_date = session.date.date()
            
            if session_date == current_date:
                streak += 1
                continue
                
            if session_date == current_date - timedelta(days=1):
                streak += 1
                current_date = session_date
            else:
                break
                
        return streak
    
    @staticmethod
    def get_badges(skill, sessions: List[PracticeSession]) -> List[str]:
        badges = []
        total_hours = skill.total_hours
        
        if total_hours >= 100:
            badges.append("Centurion (100+ hours)")
        elif total_hours >= 50:
            badges.append("Half-Centurion (50+ hours)")
        elif total_hours >= 25:
            badges.append("Silver Hour (25+ hours)")
        elif total_hours >= 10:
            badges.append("Bronze Hour (10+ hours)")
        
        streak = ProgressUtils.calculate_streak(sessions)
        if streak >= 7:
            badges.append("Weekly Streak (7+ days)")
        elif streak >= 3:
            badges.append("Getting Started (3+ days)")
        
        if skill.proficiency == "Expert":
            badges.append("Expert Level")
        elif skill.proficiency == "Advanced":
            badges.append("Advanced Level")
        
        return badges
    
    @staticmethod
    def get_weekly_progress(sessions: List[PracticeSession]) -> Dict[str, float]:
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        
        weekly_data = {}
        for i in range(7):
            day = start_of_week + timedelta(days=i)
            weekly_data[day.strftime('%A')] = 0.0
        
        for session in sessions:
            session_date = session.date.date()
            if session_date >= start_of_week:
                day_name = session_date.strftime('%A')
                weekly_data[day_name] = weekly_data.get(day_name, 0) + session.duration
        
        return weekly_data
    
    @staticmethod
    def get_monthly_progress(sessions: List[PracticeSession]) -> Dict[str, float]:
        today = datetime.now().date()
        start_of_month = today.replace(day=1)
        
        weekly_data = {}
        current_date = start_of_month
        week_num = 1
        
        while current_date.month == start_of_month.month:
            week_key = f"Week {week_num}"
            weekly_data[week_key] = 0.0
            current_date += timedelta(days=7)
            week_num += 1
        
        for session in sessions:
            session_date = session.date.date()
            if session_date >= start_of_month and session_date.month == start_of_month.month:
                week_num = (session_date.day - 1) // 7 + 1
                week_key = f"Week {week_num}"
                weekly_data[week_key] = weekly_data.get(week_key, 0) + session.duration
        
        return weekly_data
    
