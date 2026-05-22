from __future__ import annotations

from fastapi import HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware

from app.control.backpressure_manager import BackpressureManager

backpressure_manager = BackpressureManager(window_seconds=60, limit_rho=0.95)


class BackpressureMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method in {"POST", "PUT", "DELETE", "PATCH"}:
            is_overloaded = await backpressure_manager.is_overloaded()
            if is_overloaded:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="System queue overloaded. Retry with exponential backoff.",
                )
            await backpressure_manager.record_arrival()

        return await call_next(request)
        try:
            if request.method in {"POST", "PUT", "DELETE", "PATCH"}:
                is_overloaded = await backpressure_manager.is_overloaded()
                if is_overloaded:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="System queue overloaded. Retry with exponential backoff.",
                    )
                await backpressure_manager.record_arrival()

            return await call_next(request)
        except HTTPException:
            raise
        except (TypeError, ValueError) as exc:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(exc)})
