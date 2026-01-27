from fastapi import FastAPI, HTTPException
from .config import settings
from .db import init_db_pool, close_db_pool, get_pool
from .services.jobs import claim_jobs, mark_done, mark_retry

app = FastAPI(title="HumTech Worker", version="0.1.0")

@app.on_event("startup")
async def _startup():
    await init_db_pool()

@app.on_event("shutdown")
async def _shutdown():
    await close_db_pool()

@app.get("/health")
async def health():
    return {"ok": True, "service": settings.service_name, "env": settings.env}

@app.post("/worker/run")
async def worker_run(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    pool = await get_pool()
    processed = 0
    claimed = 0
    failures = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            jobs = await claim_jobs(conn, limit=limit, locked_by=settings.worker_id)
            claimed = len(jobs)

        # Process outside the claim transaction (each job can have its own tx later)
        for job in jobs:
            try:
                # TODO: load inbound_event -> upsert contact/conversation -> insert inbound message
                # TODO: LLM extract -> route -> adapters -> insert outbound -> mark done
                async with conn.transaction():
                    await mark_done(conn, job.job_id)
                processed += 1
            except Exception as e:
                failures += 1
                err = {"error": str(e), "job_id": job.job_id}
                async with conn.transaction():
                    # simple backoff: 30s; you can make this exponential later
                    await mark_retry(conn, job.job_id, delay_seconds=30, error_obj=err)

    return {"claimed": claimed, "processed": processed, "failures": failures, "worker_id": settings.worker_id}
