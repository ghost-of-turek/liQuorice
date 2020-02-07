import orm


class QueuedTask(orm.Model):
    __tablename__ = 'queued_tasks'

    id = orm.Integer(primary_key=True)
    job = orm.String(max_length=100)
    job_params = orm.JSON(default={})
