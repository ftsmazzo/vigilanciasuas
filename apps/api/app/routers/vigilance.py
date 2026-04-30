import time

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db
from ..deps import get_current_user
from ..models import User
from ..vigilance.domicilio_mview import refresh_domicilio_mview
from ..vigilance.familia_mview import bolsa_folha_kpis_from_raw, refresh_familia_mview
from ..vigilance.pessoas_mview import refresh_pessoas_mview

router = APIRouter(prefix="/vigilance", tags=["vigilance"])


@router.get("/kpis")
def get_vigilance_kpis(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """
    KPIs iniciais da vigilância:
    - total_familias (vig.mvw_familia)
    - total_pessoas (vig.mvw_pessoas)
    - total_homens / total_mulheres + percentual sobre total de pessoas
    - Bolsa Família: contagem e valores direto de raw.sibec__programa_bolsa_familia (igual agregação da MV).
    - % Bolsa sobre CADU: usa total_familias da vig.mvw_familia; TAC continua na MV.
    """
    with db.bind.begin() as conn:
        familias_exists = conn.execute(text("SELECT to_regclass('vig.mvw_familia')")).scalar()
        pessoas_exists = conn.execute(text("SELECT to_regclass('vig.mvw_pessoas')")).scalar()
        if not familias_exists or not pessoas_exists:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Views necessárias não encontradas. Gere primeiro vig.mvw_familia e vig.mvw_pessoas "
                    "na página Dados vigilância."
                ),
            )

        fam_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*)::bigint AS n_fam,
                  COUNT(*) FILTER (
                    WHERE meses_desatualizado IS NOT NULL AND meses_desatualizado <= 24
                  )::bigint AS n_tac
                FROM vig.mvw_familia
                """
            )
        ).mappings().first()
        fr = fam_row or {}
        total_familias = int(fr.get("n_fam") or 0)
        tac_familias = int(fr.get("n_tac") or 0)

        bolsa = bolsa_folha_kpis_from_raw(conn)
        total_bolsa_familia = bolsa.total_familias_folha
        total_pago_bf = bolsa.total_pago

        total_pessoas = int(conn.execute(text("SELECT COUNT(*) FROM vig.mvw_pessoas")).scalar() or 0)

        total_homens = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM vig.mvw_pessoas
                    WHERE UPPER(COALESCE(cod_sexo, '')) IN ('1', 'M', 'MASCULINO')
                    """
                )
            ).scalar()
            or 0
        )
        total_mulheres = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM vig.mvw_pessoas
                    WHERE UPPER(COALESCE(cod_sexo, '')) IN ('2', 'F', 'FEMININO')
                    """
                )
            ).scalar()
            or 0
        )

    def pct(value: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return round((value / total) * 100, 2)

    media_valor_bf = (
        round(total_pago_bf / total_bolsa_familia, 2) if total_bolsa_familia > 0 else 0.0
    )

    return {
        "total_familias": total_familias,
        "total_pessoas": total_pessoas,
        "total_homens": total_homens,
        "pct_homens": pct(total_homens, total_pessoas),
        "total_mulheres": total_mulheres,
        "pct_mulheres": pct(total_mulheres, total_pessoas),
        "total_bolsa_familia": total_bolsa_familia,
        "pct_bolsa_familia_cadu": pct(total_bolsa_familia, total_familias),
        "tac_familias": tac_familias,
        "tac_pct": pct(tac_familias, total_familias),
        "total_pago_bolsa_familia": round(total_pago_bf, 2),
        "media_valor_bolsa_familia": media_valor_bf,
    }


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


@router.post("/materialized-views/pessoas/refresh")
def refresh_pessoas(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Recria `vig.mvw_pessoas` (uma linha por membro no CADU, com idade calculada)."""
    t0 = time.perf_counter()
    try:
        with db.bind.begin() as conn:
            result = refresh_pessoas_mview(conn)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Falha ao gerar visão Pessoas: {exc}",
        ) from exc

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "status": "success",
        "view_schema": "vig",
        "view_name": "mvw_pessoas",
        "row_count": result.row_count,
        "elapsed_ms": elapsed_ms,
        "warnings": result.warnings,
    }
