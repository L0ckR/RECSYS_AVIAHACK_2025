import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


class Channel(str, Enum):
    mobile = "mobile"
    web = "web"
    crm = "crm"


class EventType(str, Enum):
    click = "click"
    dismiss = "dismiss"
    purchase = "purchase"


class RecommendationItem(BaseModel):
    product_id: str
    product_name: str
    score: float = Field(..., ge=0, le=1)
    reasons: Optional[List[str]] = None


class RecommendationResponse(BaseModel):
    user_id: str
    generated_at: datetime
    correlation_id: str
    recommendations: List[RecommendationItem]


class FeedbackRequest(BaseModel):
    rec_id: str = Field(..., min_length=1, max_length=128)
    user_id: str = Field(..., min_length=1, max_length=128)
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    context: Optional[Dict[str, Any]] = None


class FeedbackResponse(BaseModel):
    status: str
    correlation_id: str


def get_correlation_id(x_correlation_id: Optional[str] = Header(None)) -> str:
    correlation_id = x_correlation_id or str(uuid.uuid4())
    logger.info("Handling request with correlation_id=%s", correlation_id)
    return correlation_id


def create_app() -> FastAPI:
    app = FastAPI(
        title="Recommendation API",
        version="1.0.0",
        description="Stubbed recommendation API for PSB recommender platform.",
        openapi_tags=[
            {"name": "recommendations", "description": "Recommendation operations"},
            {"name": "feedback", "description": "Feedback ingestion"},
        ],
    )

    @app.get(
        "/api/v1/recommendations",
        response_model=RecommendationResponse,
        tags=["recommendations"],
    )
    async def get_recommendations(
        request: Request,
        user_id: str = Query(..., min_length=1, max_length=128),
        channel: Channel = Query(..., description="Origin channel"),
        top_k: int = Query(5, ge=1, le=50, description="Number of recommendations"),
        correlation_id: str = Depends(get_correlation_id),
    ) -> JSONResponse:
        logger.info(
            "GET /recommendations user_id=%s channel=%s top_k=%s correlation_id=%s",
            user_id,
            channel,
            top_k,
            correlation_id,
        )
        now = datetime.utcnow()
        items = [
            RecommendationItem(
                product_id=f"prod-{i+1}",
                product_name=f"Sample Product {i+1}",
                score=round(1 - (i * 0.05), 2),
                reasons=["behavioral-signal", "popularity"],
            )
            for i in range(top_k)
        ]
        response = RecommendationResponse(
            user_id=user_id,
            generated_at=now,
            correlation_id=correlation_id,
            recommendations=items,
        )
        return JSONResponse(status_code=200, content=response.model_dump(mode="json"))

    @app.post(
        "/api/v1/recommendations/feedback",
        response_model=FeedbackResponse,
        tags=["feedback"],
    )
    async def post_feedback(
        request: Request,
        payload: FeedbackRequest,
        correlation_id: str = Depends(get_correlation_id),
    ) -> JSONResponse:
        logger.info(
            "POST /recommendations/feedback rec_id=%s user_id=%s event_type=%s correlation_id=%s",
            payload.rec_id,
            payload.user_id,
            payload.event_type,
            correlation_id,
        )
        response = FeedbackResponse(status="accepted", correlation_id=correlation_id)
        return JSONResponse(status_code=202, content=response.model_dump())

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
