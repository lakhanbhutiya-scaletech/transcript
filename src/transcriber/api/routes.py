"""HTTP endpoints: health, job status and manual enqueue."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from ..container import Container
from ..domain.enums import JobStatus
from ..domain.exceptions import TranscriptionError
from .schemas import HealthResponse, SubmitJobRequest, SubmitJobResponse


def get_container(request: Request) -> Container:
    return request.app.state.container


router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["ops"])
async def health(container: Container = Depends(get_container)) -> HealthResponse:
    database_ok = await container.database.ping()
    return HealthResponse(
        status="ok" if database_ok else "degraded",
        database=database_ok,
        version=container.settings.environment,
        environment=container.settings.environment,
    )


@router.get("/queue/stats", tags=["ops"])
async def queue_stats(container: Container = Depends(get_container)) -> dict:
    return {
        "queue": container.producer.name,
        "counts": await container.producer.counts(),
    }


@router.post(
    "/jobs",
    response_model=SubmitJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["jobs"],
)
async def submit_job(
    payload: SubmitJobRequest, container: Container = Depends(get_container)
) -> SubmitJobResponse:
    """Validate the payload up front so bad input fails here, not in a worker."""
    parser = container.payload_parser
    try:
        request = parser.parse(payload.to_job_payload())
        parser.ensure_readable(request.audio_path)
    except TranscriptionError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": error.message, **error.details},
        ) from error

    queue_job_id = await container.producer.enqueue(payload.to_job_payload())
    return SubmitJobResponse(
        externalId=payload.id,
        queueJobId=queue_job_id,
        queue=container.producer.name,
    )


@router.get("/jobs/{external_id}", tags=["jobs"])
async def get_job(
    external_id: str, container: Container = Depends(get_container)
) -> dict:
    job = await container.repository.get(external_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No transcription job with id {external_id!r}",
        )
    return job


@router.get("/jobs", tags=["jobs"])
async def list_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    job_status: JobStatus | None = Query(default=None, alias="status"),
    container: Container = Depends(get_container),
) -> dict:
    jobs = await container.repository.list_recent(limit=limit, status=job_status)
    return {"count": len(jobs), "jobs": jobs}
