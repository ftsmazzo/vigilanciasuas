import time

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..db import get_db
from ..deps import get_current_user
from ..models import User
from ..vigilance.domicilio_mview import refresh_domicilio_mview
from ..vigilance.familia_mview import refresh_familia_mview

router = APIRouter(prefix="/vigilance", tags=["vigilance"])


@router.post("/materialized-views/familia/refresh")
def refresh_familia(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """
    Recria a materialized view `vig.mvw_familia` (uma linha por código familiar).
    Pode levar vários segundos em bases grandes; o cliente deve exibir estado de carregamento.
    """
    t0 = time.perf_counter()
    try:
        with db.bind.begin() as conn:
            result = refresh_familia_mview(conn)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Falha ao gerar visão Família: {exc}",
        ) from exc

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "status": "success",
        "view_schema": "vig",
        "view_name": "mvw_familia",
        "row_count": result.row_count,
        "elapsed_ms": elapsed_ms,
        "warnings": result.warnings,
        "pbf_columns_detected": {
            "codigo_familiar": result.pbf_cod_column,
            "valor": result.pbf_valor_column,
            "referencia_folha": result.pbf_ref_column,
        },
    }


@router.post("/materialized-views/familia-domicilio/refresh")
def refresh_familia_domicilio(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Recria `vig.mvw_familia_domicilio` (moradia, riscos, GPTE — uma linha por família, só CADU)."""
    t0 = time.perf_counter()
    try:
        with db.bind.begin() as conn:
            result = refresh_domicilio_mview(conn)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Falha ao gerar visão Domicílio: {exc}",
        ) from exc

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "status": "success",
        "view_schema": "vig",
        "view_name": "mvw_familia_domicilio",
        "row_count": result.row_count,
        "elapsed_ms": elapsed_ms,
        "warnings": result.warnings,
    }
