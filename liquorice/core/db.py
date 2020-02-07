from gino import Gino

from liquorice.core.const import TaskStatus


db = Gino()


class QueuedTask(db.Model):
    __tablename__ = 'queued_tasks'

    id = db.Column(db.Integer(), primary_key=True)
    job = db.Column(db.String(length=100))
    data = db.Column(db.JSON(), default={})
    due_at = db.Column(db.DateTime())
    status = db.Column(db.Enum(TaskStatus))
