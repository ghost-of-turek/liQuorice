from typing import Dict, Optional

import attr


@attr.s(frozen=True)
class Job:
    async def run(self):
        pass


@attr.s
class Registry:
    _jobs: Dict[str, Job] = attr.ib(default=attr.Factory(dict))

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    def job(self, job: Job) -> None:
        self._jobs[job.name(), job]

    def get(self, name: str) -> Optional[Job]:
        return self._jobs.get(name, default=None)
