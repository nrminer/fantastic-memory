from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Player(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    balance = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    transactions = db.relationship('Transaction', backref='player', lazy=True)

    @property
    def total_profit(self):
        return sum(t.amount for t in self.transactions if t.amount > 0)

    @property
    def total_loss(self):
        return sum(t.amount for t in self.transactions if t.amount < 0)


class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    note = db.Column(db.String(200), default='')
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
