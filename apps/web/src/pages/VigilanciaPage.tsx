import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

type Props = {
  token: string;
};

type VigilanciaTab = "familia" | "domicilio";

type FamiliaRefreshResponse = {
  status: string;
  view_schema: string;
  view_name: string;
  row_count: number;
  elapsed_ms: number;
  warnings: string[];
  pbf_columns_detected: {
    codigo_familiar: string | null;
    valor: string | null;
    referencia_folha: string | null;
  };
};

type DomicilioRefreshResponse = {
  status: string;
  view_schema: string;
  view_name: string;
  row_count: number;
  elapsed_ms: number;
  warnings: string[];
};

export default function VigilanciaPage({ token }: Props) {
  const [tab, setTab] = useState<VigilanciaTab>("familia");
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState("");
  const [familiaResult, setFamiliaResult] = useState<FamiliaRefreshResponse | null>(null);
  const [domicilioResult, setDomicilioResult] = useState<DomicilioRefreshResponse | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

  function startProgressAnimation() {
    setProgress(8);
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = setInterval(() => {
      setProgress((p) => {
        if (p >= 92) return p;
        return p + Math.max(1, Math.round((92 - p) * 0.07));
      });
    }, 320);
  }

  function stopProgressAnimation(final: number) {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
    setProgress(final);
  }

  async function refreshFamilia() {
    setError("");
    setFamiliaResult(null);
    setBusy(true);
    startProgressAnimation();
    try {
      const response = await fetch(`${API_URL}/api/v1/vigilance/materialized-views/familia/refresh`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = (await response.json().catch(() => ({}))) as FamiliaRefreshResponse & { detail?: unknown };
      if (!response.ok) {
        const msg =
          typeof data.detail === "string"
            ? data.detail
            : Array.isArray(data.detail)
              ? JSON.stringify(data.detail)
              : "Falha ao gerar a visão Família.";
        throw new Error(msg);
      }
      stopProgressAnimation(100);
      setFamiliaResult(data);
    } catch (e) {
      stopProgressAnimation(0);
      setError(e instanceof Error ? e.message : "Erro inesperado.");
    } finally {
      setBusy(false);
    }
  }

  async function refreshDomicilio() {
    setError("");
    setDomicilioResult(null);
    setBusy(true);
    startProgressAnimation();
    try {
      const response = await fetch(`${API_URL}/api/v1/vigilance/materialized-views/familia-domicilio/refresh`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = (await response.json().catch(() => ({}))) as DomicilioRefreshResponse & { detail?: unknown };
      if (!response.ok) {
        const msg =
          typeof data.detail === "string"
            ? data.detail
            : Array.isArray(data.detail)
              ? JSON.stringify(data.detail)
              : "Falha ao gerar a visão Domicílio.";
        throw new Error(msg);
      }
      stopProgressAnimation(100);
      setDomicilioResult(data);
    } catch (e) {
      stopProgressAnimation(0);
      setError(e instanceof Error ? e.message : "Erro inesperado.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="ingestao-page">
      <aside className="ingestao-sidebar" aria-label="Visões analíticas">
        <div className="ingestao-sidebar-head">
          <h2>Dados vigilância</h2>
          <p className="ingestao-sidebar-sub">Views materializadas para análise</p>
        </div>
        <nav className="ingestao-nav">
          <button
            type="button"
            className={`ingestao-nav-item ${tab === "familia" ? "active" : ""}`}
            onClick={() => {
              setError("");
              setTab("familia");
            }}
          >
            <span className="ingestao-nav-label">Família</span>
            <span className="ingestao-nav-hint">CADU + folha Bolsa Família</span>
          </button>
          <button
            type="button"
            className={`ingestao-nav-item ${tab === "domicilio" ? "active" : ""}`}
            onClick={() => {
              setError("");
              setTab("domicilio");
            }}
          >
            <span className="ingestao-nav-label">Domicílio</span>
            <span className="ingestao-nav-hint">Moradia, riscos e GPTE (CADU)</span>
          </button>
        </nav>
        <Link to="/" className="ingestao-back">
          ← Voltar ao painel
        </Link>
      </aside>

      <div className="ingestao-main-stack">
        <main className="ingestao-content">
          {tab === "familia" && (
            <section className="ingestao-panel">
              <h1>Visão materializada — Família</h1>
              <p className="ingestao-desc">
                Uma linha por código familiar (sem nomes de pessoas). A folha de pagamento só traz famílias que
                recebem benefício; o valor vem da coluna <strong>vlrtotal</strong> (soma por família). Use a ingestão
                com <strong>competência AAAAMM</strong>: a visão usa o mês mais recente gravado em{" "}
                <code className="inline-code">competencia</code> na RAW. <strong>marc_pbf</strong> é <em>true</em> se a
                família aparece nesse recorte. O indicador do CADU fica em <strong>marc_pbf_cadu</strong>. Tabela:{" "}
                <strong>vig.mvw_familia</strong>.
              </p>

              <div className="vig-actions">
                <button type="button" onClick={() => void refreshFamilia()} disabled={busy}>
                  {busy ? "Gerando visão…" : "Gerar / atualizar visão Família"}
                </button>
              </div>

              <div className="progress-wrap" aria-live="polite">
                <div className="progress-track">
                  <div className="progress-fill" style={{ width: `${progress}%` }} />
                </div>
                <small>
                  {busy
                    ? "Recriando materialized view no PostgreSQL…"
                    : progress === 100
                      ? "Concluído."
                      : "Aguardando comando."}
                </small>
              </div>

              {error && <p className="error">{error}</p>}

              {familiaResult && (
                <div className="vig-result">
                  <p className="status-ok" style={{ marginTop: "0.75rem" }}>
                    Visão <code className="inline-code">vig.{familiaResult.view_name}</code> atualizada:{" "}
                    <strong>{familiaResult.row_count.toLocaleString("pt-BR")}</strong> famílias em{" "}
                    {(familiaResult.elapsed_ms / 1000).toLocaleString("pt-BR", {
                      minimumFractionDigits: 1,
                      maximumFractionDigits: 1,
                    })}
                    s.
                  </p>
                  {familiaResult.warnings.length > 0 && (
                    <ul className="vig-warnings">
                      {familiaResult.warnings.map((w) => (
                        <li key={w}>{w}</li>
                      ))}
                    </ul>
                  )}
                  <p className="ingestao-desc" style={{ marginBottom: 0 }}>
                    Colunas detectadas na folha PBF: código{" "}
                    <code className="inline-code">{familiaResult.pbf_columns_detected.codigo_familiar ?? "—"}</code>,
                    valor <code className="inline-code">{familiaResult.pbf_columns_detected.valor ?? "—"}</code>,
                    referência{" "}
                    <code className="inline-code">{familiaResult.pbf_columns_detected.referencia_folha ?? "—"}</code>.
                  </p>
                </div>
              )}
            </section>
          )}

          {tab === "domicilio" && (
            <section className="ingestao-panel">
              <h1>Visão materializada — Domicílio</h1>
              <p className="ingestao-desc">
                Uma linha por <strong>codigo_familiar</strong>, só a partir do <code className="inline-code">
                  raw.cecad__cadu
                </code>
                : situação do domicílio, materiais, saneamento, indígena/quilombola, CRAS, riscos (violência de direitos,
                insegurança alimentar) e GPTE. <strong>total_pessoas</strong> é a contagem de CPFs distintos (11
                dígitos) entre as linhas da mesma família. Tabela: <strong>vig.mvw_familia_domicilio</strong>. Atualize
                quando recarregar o CADU.
              </p>

              <div className="vig-actions">
                <button type="button" onClick={() => void refreshDomicilio()} disabled={busy}>
                  {busy ? "Gerando visão…" : "Gerar / atualizar visão Domicílio"}
                </button>
              </div>

              <div className="progress-wrap" aria-live="polite">
                <div className="progress-track">
                  <div className="progress-fill" style={{ width: `${progress}%` }} />
                </div>
                <small>
                  {busy
                    ? "Recriando materialized view no PostgreSQL…"
                    : progress === 100
                      ? "Concluído."
                      : "Aguardando comando."}
                </small>
              </div>

              {error && <p className="error">{error}</p>}

              {domicilioResult && (
                <div className="vig-result">
                  <p className="status-ok" style={{ marginTop: "0.75rem" }}>
                    Visão <code className="inline-code">vig.{domicilioResult.view_name}</code> atualizada:{" "}
                    <strong>{domicilioResult.row_count.toLocaleString("pt-BR")}</strong> famílias em{" "}
                    {(domicilioResult.elapsed_ms / 1000).toLocaleString("pt-BR", {
                      minimumFractionDigits: 1,
                      maximumFractionDigits: 1,
                    })}
                    s.
                  </p>
                  {domicilioResult.warnings.length > 0 && (
                    <ul className="vig-warnings">
                      {domicilioResult.warnings.map((w) => (
                        <li key={w}>{w}</li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </section>
          )}
        </main>
      </div>
    </div>
  );
}
