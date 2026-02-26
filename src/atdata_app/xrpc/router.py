"""Combined XRPC router mounting all query, procedure, and subscription endpoints."""

from fastapi import APIRouter

from atdata_app.xrpc.procedures import router as procedures_router
from atdata_app.xrpc.queries import router as queries_router
from atdata_app.xrpc.subscriptions import router as subscriptions_router

router = APIRouter(prefix="/xrpc")
router.include_router(queries_router)
router.include_router(procedures_router)
router.include_router(subscriptions_router)
