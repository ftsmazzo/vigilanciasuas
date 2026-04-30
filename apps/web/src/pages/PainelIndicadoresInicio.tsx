import { useEffect, useState } from "react";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

type Props = {
  token: string;
};

type VigilanciaKpis = {
  total_familias: number;
  total_pessoas: number;
  total_homens: number;
  pct_homens: number;
  total_mulheres: number;
  pct_mulheres: number;
  total_bolsa_familia: number;
  pct_bolsa_familia_cadu: number;
  tac_familias: number;
  tac_pct: number;
  total_pago_bolsa_familia: number;
  media_valor_bolsa_familia: number;
  renda_ate_218_familias: number;
  renda_ate_218_pct: number;
  renda_219_706_familias: number;
  renda_219_706_pct: number;
  renda_acima_706_familias: number;
  renda_acima_706_pct: number;
};

const brl = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

/** Painel de KPIs exibido na página Início (dados Cadastro Único). */
export default function PainelIndicadoresInicio({ token }: Props) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [kpis, setKpis] = useState<VigilanciaKpis | null>(null);

  async function loadKpis() {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${API_URL}/api/v1/vigilance/kpis`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = (await response.json().catch(() => ({}))) as VigilanciaKpis & { detail?: unknown };
      if (!response.ok) {
        const msg =
          typeof data.detail === "string"
            ? data.detail
            : Array.isArray(data.detail)
              ? JSON.stringify(data.detail)
              : "Falha ao carregar indicadores.";
        throw new Error(msg);
      }
      setKpis(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Erro inesperado ao consultar indicadores.");
      setKpis(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadKpis();
  }, []);

  return (
    <section className="kpi-page">
      <div className="kpi-head">
        <h1>Indicadores</h1>
        <p>Cadastro Único (CECAD) e folha Bolsa Família.</p>
        <button type="button" onClick={() => void loadKpis()} disabled={loading}>
          {loading ? "Atualizando..." : "Atualizar indicadores"}
        </button>
      </div>

      {error && <p className="error">{error}</p>}

      {!error && kpis && (
        <>
          <h2 className="kpi-section-title">Famílias e Pessoas</h2>
          <div className="kpi-grid" aria-label="Indicadores do Cadastro Único">
            <article className="kpi-card">
              <small>Total de famílias</small>
              <strong>{kpis.total_familias.toLocaleString("pt-BR")}</strong>
              <span>Dados Cadastro Único — Família</span>
            </article>
            <article className="kpi-card">
              <small>Total de pessoas</small>
              <strong>{kpis.total_pessoas.toLocaleString("pt-BR")}</strong>
              <span>Dados Cadastro Único — Pessoa</span>
            </article>
            <article className="kpi-card">
              <small>Total de homens</small>
              <strong>{kpis.total_homens.toLocaleString("pt-BR")}</strong>
              <span>{kpis.pct_homens.toLocaleString("pt-BR")} % (Cadastro Único)</span>
            </article>
            <article className="kpi-card">
              <small>Total de mulheres</small>
              <strong>{kpis.total_mulheres.toLocaleString("pt-BR")}</strong>
              <span>{kpis.pct_mulheres.toLocaleString("pt-BR")} % (Cadastro Único)</span>
            </article>
          </div>

          <h2 className="kpi-section-title">Bolsa Família e TAC</h2>
          <div className="kpi-grid" aria-label="Bolsa Família e TAC">
            <article className="kpi-card">
              <small>Famílias na folha Bolsa Família</small>
              <strong>{kpis.total_bolsa_familia.toLocaleString("pt-BR")}</strong>
              <span>
                {kpis.pct_bolsa_familia_cadu.toLocaleString("pt-BR")} % das famílias no Cadastro Único
              </span>
            </article>
            <article className="kpi-card">
              <small>TAC</small>
              <strong>{kpis.tac_pct.toLocaleString("pt-BR")} %</strong>
              <span>{kpis.tac_familias.toLocaleString("pt-BR")} famílias</span>
            </article>
            <article className="kpi-card">
              <small>Total pago (Bolsa Família)</small>
              <strong>{brl.format(kpis.total_pago_bolsa_familia)}</strong>
              <span>Total na folha</span>
            </article>
            <article className="kpi-card">
              <small>Média por família na folha</small>
              <strong>{brl.format(kpis.media_valor_bolsa_familia)}</strong>
              <span>Média por família beneficiária</span>
            </article>
          </div>

          <h2 className="kpi-section-title">Renda Per Capita</h2>
          <div className="kpi-grid kpi-grid-3" aria-label="Faixas de renda per capita">
            <article className="kpi-card">
              <small>Renda per capita de 0,00 até 218</small>
              <strong>{kpis.renda_ate_218_familias.toLocaleString("pt-BR")}</strong>
              <span>{kpis.renda_ate_218_pct.toLocaleString("pt-BR")} % das famílias do Cadastro Único</span>
            </article>
            <article className="kpi-card">
              <small>Renda per capita de 219 até 706</small>
              <strong>{kpis.renda_219_706_familias.toLocaleString("pt-BR")}</strong>
              <span>{kpis.renda_219_706_pct.toLocaleString("pt-BR")} % das famílias do Cadastro Único</span>
            </article>
            <article className="kpi-card">
              <small>Renda per capita acima de 706</small>
              <strong>{kpis.renda_acima_706_familias.toLocaleString("pt-BR")}</strong>
              <span>{kpis.renda_acima_706_pct.toLocaleString("pt-BR")} % das famílias do Cadastro Único</span>
            </article>
          </div>
        </>
      )}
    </section>
  );
}
