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
  tac_familias_24m: number;
  tac_pct: number;
  total_pago_bolsa_familia: number;
  media_valor_bolsa_familia: number;
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
        <p>Cadastro Único (CECAD), folha Bolsa Família e indicadores de atualização cadastral.</p>
        <button type="button" onClick={() => void loadKpis()} disabled={loading}>
          {loading ? "Atualizando..." : "Atualizar indicadores"}
        </button>
      </div>

      {error && <p className="error">{error}</p>}

      {!error && kpis && (
        <>
          <div className="kpi-grid" aria-label="Indicadores do Cadastro Único">
            <article className="kpi-card">
              <small>Total de famílias</small>
              <strong>{kpis.total_familias.toLocaleString("pt-BR")}</strong>
              <span>Dados Cadastro Único — famílias</span>
            </article>
            <article className="kpi-card">
              <small>Total de pessoas</small>
              <strong>{kpis.total_pessoas.toLocaleString("pt-BR")}</strong>
              <span>Dados Cadastro Único — pessoas</span>
            </article>
            <article className="kpi-card">
              <small>Total de homens</small>
              <strong>{kpis.total_homens.toLocaleString("pt-BR")}</strong>
              <span>
                {kpis.pct_homens.toLocaleString("pt-BR")} % do total de pessoas (Cadastro Único)
              </span>
            </article>
            <article className="kpi-card">
              <small>Total de mulheres</small>
              <strong>{kpis.total_mulheres.toLocaleString("pt-BR")}</strong>
              <span>
                {kpis.pct_mulheres.toLocaleString("pt-BR")} % do total de pessoas (Cadastro Único)
              </span>
            </article>
          </div>

          <h2 className="kpi-section-title">Bolsa Família e TAC</h2>
          <div className="kpi-grid" aria-label="Bolsa Família e taxa de atualização cadastral">
            <article className="kpi-card">
              <small>Famílias na folha Bolsa Família</small>
              <strong>{kpis.total_bolsa_familia.toLocaleString("pt-BR")}</strong>
              <span>
                {kpis.pct_bolsa_familia_cadu.toLocaleString("pt-BR")} % das famílias no Cadastro Único
              </span>
            </article>
            <article className="kpi-card">
              <small>TAC — atualização cadastral (24 meses)</small>
              <strong>{kpis.tac_pct.toLocaleString("pt-BR")} %</strong>
              <span>
                {kpis.tac_familias_24m.toLocaleString("pt-BR")} famílias com data de atualização nos últimos
                24 meses
              </span>
            </article>
            <article className="kpi-card">
              <small>Total pago (Bolsa Família)</small>
              <strong>{brl.format(kpis.total_pago_bolsa_familia)}</strong>
              <span>Soma dos valores na folha consolidada (município)</span>
            </article>
            <article className="kpi-card">
              <small>Média por família na folha</small>
              <strong>{brl.format(kpis.media_valor_bolsa_familia)}</strong>
              <span>Valor médio entre famílias com pagamento na folha</span>
            </article>
          </div>
        </>
      )}
    </section>
  );
}
