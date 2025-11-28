import logging
import uuid
from concurrent import futures
from datetime import datetime
from typing import Dict, Iterable, Tuple

import grpc

from proto.generated import recommendation_pb2, recommendation_pb2_grpc

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


def _extract_correlation_id(metadata: Iterable[Tuple[str, str]], fallback: str) -> str:
    metadata_dict: Dict[str, str] = {key.lower(): value for key, value in metadata}
    return metadata_dict.get("x-correlation-id", fallback)


class RecommendationService(recommendation_pb2_grpc.RecommendationServiceServicer):
    def GetRecommendations(self, request, context):
        correlation_id = _extract_correlation_id(
            context.invocation_metadata(), request.correlation_id or str(uuid.uuid4())
        )
        logger.info(
            "gRPC GetRecommendations user_id=%s channel=%s top_k=%s correlation_id=%s",
            request.user_id,
            request.channel,
            request.top_k,
            correlation_id,
        )
        recommendations = [
            recommendation_pb2.RecommendationItem(
                product_id=f"prod-{i+1}",
                product_name=f"Sample Product {i+1}",
                score=max(0.0, 1 - (i * 0.05)),
                reasons=["behavioral-signal", "popularity"],
            )
            for i in range(max(1, request.top_k))
        ]
        return recommendation_pb2.RecommendationResponse(
            user_id=request.user_id,
            generated_at=datetime.utcnow().isoformat() + "Z",
            correlation_id=correlation_id,
            recommendations=recommendations,
        )

    def SendFeedback(self, request, context):
        correlation_id = _extract_correlation_id(
            context.invocation_metadata(), request.correlation_id or str(uuid.uuid4())
        )
        logger.info(
            "gRPC SendFeedback rec_id=%s user_id=%s event_type=%s correlation_id=%s",
            request.rec_id,
            request.user_id,
            request.event_type,
            correlation_id,
        )
        return recommendation_pb2.FeedbackAck(status="accepted", correlation_id=correlation_id)


def serve(host: str = "0.0.0.0", port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    recommendation_pb2_grpc.add_RecommendationServiceServicer_to_server(
        RecommendationService(), server
    )
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    logger.info("Starting gRPC RecommendationService on %s", address)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
