import pkg_resources

from liquorice.core import Task, Job, Timetable


pkg_resources.declare_namespace('liquorice')

__all__ = ['Task', 'Job', 'Timetable']
