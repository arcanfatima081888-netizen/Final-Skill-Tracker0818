from datetime import datetime
from dataclasses import dataclass
from typing import Optional

@dataclass
class Skill:
    id: int
    name: str
    category: str
    proficiency: str
    notes: str
    total_hours: float
    last_practiced: Optional[datetime]
    created_date: datetime
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'category': self.category,
            'proficiency': self.proficiency,
            'notes': self.notes,
            'total_hours': self.total_hours,
            'last_practiced': self.last_practiced.strftime('%Y-%m-%d %H:%M') if self.last_practiced else 'Never',
            'created_date': self.created_date.strftime('%Y-%m-%d')
        }

@dataclass
class PracticeSession:
    id: int
    skill_id: int
    date: datetime
    duration: float
    notes: str
    skill_name: str = None
    
    def to_dict(self):
        return {
            'id': self.id,
            'skill_id': self.skill_id,
            'date': self.date.strftime('%Y-%m-%d %H:%M'),
            'duration': self.duration,
            'notes': self.notes,
            'skill_name': self.skill_name
        }
 
