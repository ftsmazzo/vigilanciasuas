import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

type TabId = "cadu" | "pbf" | "bpc" | "sibec";

type UploadResult = { ok: boolean; data: Record<string, unknown>; errorText?: string };

function formatIngestionError(data: Record<string, unknown>): string {
  const detail = data.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail.map((item) => JSON.stringify(item)).join(" ");
  }
  return "Falha na ingestão. Confirme formato, dados e sessão.";
}

function runIngestionUpload(
  formData: FormData,
  token: string,
  onProgress: (pct: number) => void
): Promise<UploadResult> {
  return new Promise((resolve) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API_URL}/api/v1/ingestion/import`);
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);

    xhr.upload.onprogress = (ev) => {
      if (!ev.lengthComputable) return;
      onProgress(Math.round((ev.loaded / ev.total) * 100));
    };

    xhr.onload = () => {
      try {
        const data = xhr.responseText ? JSON.parse(xhr.responseText) : {};
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve({ ok: true, data });
        } else {
          resolve({ ok: false, data, errorText: formatIngestionError(data) });
        }
      } catch {
        resolve({ ok: false, data: {}, errorText: "Falha ao interpretar resposta do servidor." });
      }
    };

    xhr.onerror = () => {
      resolve({ ok: false, data: {}, errorText: "Erro de rede durante a ingestão." });
    };

    xhr.send(formData);
  });
}

type Props = {
  token: string;
};

type IngestionRunRow = {
  id: number;
  source: string;
  dataset: string;
  target_table: string;
  strategy: string;
  file_name: string;
  status: string;
  row_count: number;
  created_by_email: string;
  created_at: string;
  finished_at: string | null;
  error_message: string | null;
};

function formatRunDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" });
}

/** Uma linha por tabela RAW: mantém só a execução mais recente de cada `target_table`. */
function latestRunPerBase(runs: IngestionRunRow[]): IngestionRunRow[] {
  const sorted = [...runs].sort((a, b) => {
    const ta = new Date(a.finished_at ?? a.created_at).getTime();
    const tb = new Date(b.finished_at ?? b.created_at).getTime();
    return tb - ta;
  });
  const seen = new Set<string>();
  const out: IngestionRunRow[] = [];
  for (const run of sorted) {
    const key = run.target_table;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(run);
  }
  return out.sort((a, b) => a.target_table.localeCompare(b.target_table, "pt-BR"));
}

export default function IngestaoPage({ token }: Props) {
  const [tab, setTab] = useState<TabId>("cadu");
  const [runs, setRuns] = useState<IngestionRunRow[]>([]);
  const [runsLoading, setRunsLoading] = useState(false);
  const [runsError, setRunsError] = useState("");

  const loadRuns = useCallback(async () => {
    setRunsLoading(true);
    setRunsError("");
    try {
      const response = await fetch(`${API_URL}/api/v1/ingestion/runs`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!response.ok) {
        throw new Error("Falha ao listar execuções");
      }
      const data = (await response.json()) as IngestionRunRow[];
      setRuns(data);
    } catch {
      setRunsError("Não foi possível carregar o histórico de ingestões.");
      setRuns([]);
    } finally {
      setRunsLoading(false);
    }
  }, [token]);

  useEffect(() => {
    loadRuns();
  }, [loadRuns]);

  const runsByBase = useMemo(() => latestRunPerBase(runs), [runs]);

  // CADU
  const [caduFile, setCaduFile] = useState<File | null>(null);
  const [caduStrategy, setCaduStrategy] = useState("replace");
  const [caduStatus, setCaduStatus] = useState("");
  const [caduProgress, setCaduProgress] = useState(0);
  const [caduUploading, setCaduUploading] = useState(false);

  // Bolsa Família
  const [pbfFile, setPbfFile] = useState<File | null>(null);
  const [pbfStrategy, setPbfStrategy] = useState("replace");
  const [pbfStatus, setPbfStatus] = useState("");
  const [pbfProgress, setPbfProgress] = useState(0);
  const [pbfUploading, setPbfUploading] = useState(false);

  // BPC
  const [bpcFile, setBpcFile] = useState<File | null>(null);
  const [bpcStrategy, setBpcStrategy] = useState("replace");
  const [bpcStatus, setBpcStatus] = useState("");
  const [bpcProgress, setBpcProgress] = useState(0);
  const [bpcUploading, setBpcUploading] = useState(false);

  // SIBEC manutenções
  const [sibecFile, setSibecFile] = useState<File | null>(null);
  const [sibecStrategy, setSibecStrategy] = useState("append");
  const [sibecCompetencia, setSibecCompetencia] = useState("");
  const [sibecOverwrite, setSibecOverwrite] = useState(false);
  const [sibecStatus, setSibecStatus] = useState("");
  const [sibecProgress, setSibecProgress] = useState(0);
  const [sibecUploading, setSibecUploading] = useState(false);

  async function submitCadu(e: FormEvent) {
    e.preventDefault();
    setCaduStatus("");
    setCaduProgress(0);
    if (!caduFile) {
      setCaduStatus("Selecione um arquivo CSV ou XLSX.");
      return;
    }
    const fd = new FormData();
    fd.append("file", caduFile);
    fd.append("source", "cecad");
    fd.append("dataset", "cadu");
    fd.append("strategy", caduStrategy);
    fd.append("csv_delimiter", ";");
    setCaduUploading(true);
    const res = await runIngestionUpload(fd, token, setCaduProgress);
    setCaduUploading(false);
    if (res.ok) {
      setCaduProgress(100);
      setCaduStatus(`Dados inseridos com sucesso. Total de ${res.data.row_count} registros.`);
      setCaduFile(null);
      await loadRuns();
    } else {
      setCaduStatus(res.errorText || "Falha na ingestão.");
    }
  }

  async function submitPbf(e: FormEvent) {
    e.preventDefault();
    setPbfStatus("");
    setPbfProgress(0);
    if (!pbfFile) {
      setPbfStatus("Selecione um arquivo CSV ou XLSX.");
      return;
    }
    const fd = new FormData();
    fd.append("file", pbfFile);
    fd.append("source", "sibec");
    fd.append("dataset", "programa_bolsa_familia");
    fd.append("strategy", pbfStrategy);
    fd.append("csv_delimiter", ";");
    setPbfUploading(true);
    const res = await runIngestionUpload(fd, token, setPbfProgress);
    setPbfUploading(false);
    if (res.ok) {
      setPbfProgress(100);
      setPbfStatus(`Dados inseridos com sucesso. Total de ${res.data.row_count} registros na base Bolsa Família.`);
      setPbfFile(null);
      await loadRuns();
    } else {
      setPbfStatus(res.errorText || "Falha na ingestão.");
    }
  }

  async function submitBpc(e: FormEvent) {
    e.preventDefault();
    setBpcStatus("");
    setBpcProgress(0);
    if (!bpcFile) {
      setBpcStatus("Selecione um arquivo CSV ou XLSX.");
      return;
    }
    const fd = new FormData();
    fd.append("file", bpcFile);
    fd.append("source", "bpc");
    fd.append("dataset", "beneficio_prestacao_continuada");
    fd.append("strategy", bpcStrategy);
    fd.append("csv_delimiter", ";");
    setBpcUploading(true);
    const res = await runIngestionUpload(fd, token, setBpcProgress);
    setBpcUploading(false);
    if (res.ok) {
      setBpcProgress(100);
      setBpcStatus(`Dados inseridos com sucesso. Total de ${res.data.row_count} registros na base BPC.`);
      setBpcFile(null);
      await loadRuns();
    } else {
      setBpcStatus(res.errorText || "Falha na ingestão.");
    }
  }

  async function submitSibec(e: FormEvent) {
    e.preventDefault();
    setSibecStatus("");
    setSibecProgress(0);
    if (!sibecFile) {
      setSibecStatus("Selecione um arquivo CSV ou XLSX.");
      return;
    }
    if (!/^\d{6}$/.test(sibecCompetencia)) {
      setSibecStatus("Informe a competência no formato AAAAMM. Ex.: 202505");
      return;
    }
    const fd = new FormData();
    fd.append("file", sibecFile);
    fd.append("source", "sibec");
    fd.append("dataset", "manutencoes");
    fd.append("strategy", sibecStrategy);
    fd.append("csv_delimiter", ";");
    fd.append("competencia", sibecCompetencia);
    fd.append("overwrite_competencia", sibecOverwrite ? "true" : "false");
    setSibecUploading(true);
    const res = await runIngestionUpload(fd, token, setSibecProgress);
    setSibecUploading(false);
    if (res.ok) {
      setSibecProgress(100);
      setSibecStatus(
        `Dados inseridos com sucesso. Competência ${String(res.data.competencia)}. Total de ${res.data.row_count} registros.`
      );
      setSibecFile(null);
      await loadRuns();
    } else {
      setSibecStatus(res.errorText || "Falha na ingestão.");
    }
  }

  const tabs: { id: TabId; label: string; hint: string }[] = [
    { id: "cadu", label: "CADU", hint: "Cadastro Único (CECAD)" },
    { id: "pbf", label: "Bolsa Família", hint: "Programa Bolsa Família" },
    { id: "bpc", label: "BPC", hint: "Benefício Prestação Continuada" },
    { id: "sibec", label: "SIBEC Manutenções", hint: "Analíticos mensais por competência" },
  ];

  return (
    <div className="ingestao-page">
      <aside className="ingestao-sidebar" aria-label="Fontes de dados">
        <div className="ingestao-sidebar-head">
          <h2>Ingestão RAW</h2>
          <p className="ingestao-sidebar-sub">Envio de arquivos para tabelas brutas no PostgreSQL</p>
        </div>
        <nav className="ingestao-nav">
          {tabs.map((t) => (
            <button
              key={t.id}
              type="button"
              className={`ingestao-nav-item ${tab === t.id ? "active" : ""}`}
              onClick={() => setTab(t.id)}
            >
              <span className="ingestao-nav-label">{t.label}</span>
              <span className="ingestao-nav-hint">{t.hint}</span>
            </button>
          ))}
        </nav>
        <Link to="/" className="ingestao-back">
          ← Voltar ao painel
        </Link>
      </aside>

      <main className="ingestao-content">
        {tab === "cadu" && (
          <section className="ingestao-panel">
            <h1>CADU — Cadastro Único</h1>
            <p className="ingestao-desc">Tabela alvo: <code>raw.cecad__cadu</code></p>
            <form onSubmit={submitCadu} className="auth-form">
              <label>
                Estratégia
                <select value={caduStrategy} onChange={(ev) => setCaduStrategy(ev.target.value)} disabled={caduUploading}>
                  <option value="replace">Substituir tabela inteira</option>
                  <option value="append">Agregar linhas</option>
                </select>
              </label>
              <label>
                Arquivo (CSV ou XLSX)
                <input
                  type="file"
                  accept=".csv,.xlsx"
                  onChange={(ev) => setCaduFile(ev.target.files?.[0] || null)}
                  disabled={caduUploading}
                  required
                />
              </label>
              <button type="submit" disabled={caduUploading}>
                {caduUploading ? "Processando…" : "Processar CADU"}
              </button>
            </form>
            <div className="progress-wrap" aria-live="polite">
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${caduProgress}%` }} />
              </div>
              <small>{caduUploading ? `Enviando: ${caduProgress}%` : "Aguardando envio"}</small>
            </div>
            {caduStatus && <p className={caduStatus.includes("sucesso") ? "status-ok" : "error"}>{caduStatus}</p>}
          </section>
        )}

        {tab === "pbf" && (
          <section className="ingestao-panel">
            <h1>Programa Bolsa Família</h1>
            <p className="ingestao-desc">Tabela alvo: <code>raw.sibec__programa_bolsa_familia</code></p>
            <form onSubmit={submitPbf} className="auth-form">
              <label>
                Estratégia
                <select value={pbfStrategy} onChange={(ev) => setPbfStrategy(ev.target.value)} disabled={pbfUploading}>
                  <option value="replace">Substituir tabela inteira</option>
                  <option value="append">Agregar linhas</option>
                </select>
              </label>
              <label>
                Arquivo (CSV ou XLSX)
                <input
                  type="file"
                  accept=".csv,.xlsx"
                  onChange={(ev) => setPbfFile(ev.target.files?.[0] || null)}
                  disabled={pbfUploading}
                  required
                />
              </label>
              <button type="submit" disabled={pbfUploading}>
                {pbfUploading ? "Processando…" : "Processar Bolsa Família"}
              </button>
            </form>
            <div className="progress-wrap" aria-live="polite">
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${pbfProgress}%` }} />
              </div>
              <small>{pbfUploading ? `Enviando: ${pbfProgress}%` : "Aguardando envio"}</small>
            </div>
            {pbfStatus && <p className={pbfStatus.includes("sucesso") ? "status-ok" : "error"}>{pbfStatus}</p>}
          </section>
        )}

        {tab === "bpc" && (
          <section className="ingestao-panel">
            <h1>BPC — Benefício de Prestação Continuada</h1>
            <p className="ingestao-desc">Tabela alvo: <code>raw.bpc__beneficio_prestacao_continuada</code></p>
            <form onSubmit={submitBpc} className="auth-form">
              <label>
                Estratégia
                <select value={bpcStrategy} onChange={(ev) => setBpcStrategy(ev.target.value)} disabled={bpcUploading}>
                  <option value="replace">Substituir tabela inteira</option>
                  <option value="append">Agregar linhas</option>
                </select>
              </label>
              <label>
                Arquivo (CSV ou XLSX)
                <input
                  type="file"
                  accept=".csv,.xlsx"
                  onChange={(ev) => setBpcFile(ev.target.files?.[0] || null)}
                  disabled={bpcUploading}
                  required
                />
              </label>
              <button type="submit" disabled={bpcUploading}>
                {bpcUploading ? "Processando…" : "Processar BPC"}
              </button>
            </form>
            <div className="progress-wrap" aria-live="polite">
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${bpcProgress}%` }} />
              </div>
              <small>{bpcUploading ? `Enviando: ${bpcProgress}%` : "Aguardando envio"}</small>
            </div>
            {bpcStatus && <p className={bpcStatus.includes("sucesso") ? "status-ok" : "error"}>{bpcStatus}</p>}
          </section>
        )}

        {tab === "sibec" && (
          <section className="ingestao-panel">
            <h1>SIBEC — Manutenções mensais</h1>
            <p className="ingestao-desc">
              Tabela alvo: <code>raw.sibec__manutencoes</code> — informe a competência de cada arquivo (carga inicial: 12 meses em <strong>append</strong>; depois, um mês por vez).
            </p>
            <form onSubmit={submitSibec} className="auth-form">
              <label>
                Competência (AAAAMM)
                <input
                  type="text"
                  value={sibecCompetencia}
                  onChange={(ev) => setSibecCompetencia(ev.target.value)}
                  placeholder="202505"
                  maxLength={6}
                  disabled={sibecUploading}
                  required
                />
              </label>
              <label>
                Estratégia
                <select
                  value={sibecStrategy}
                  onChange={(ev) => setSibecStrategy(ev.target.value)}
                  disabled={sibecUploading}
                >
                  <option value="replace">Substituir tabela inteira</option>
                  <option value="append">Agregar linhas (recomendado)</option>
                </select>
              </label>
              <label className="checkbox-inline">
                <input
                  type="checkbox"
                  checked={sibecOverwrite}
                  onChange={(ev) => setSibecOverwrite(ev.target.checked)}
                  disabled={sibecUploading}
                />
                Sobrescrever esta competência se já existir
              </label>
              <label>
                Arquivo (CSV ou XLSX)
                <input
                  type="file"
                  accept=".csv,.xlsx"
                  onChange={(ev) => setSibecFile(ev.target.files?.[0] || null)}
                  disabled={sibecUploading}
                  required
                />
              </label>
              <button type="submit" disabled={sibecUploading}>
                {sibecUploading ? "Processando…" : "Processar manutenções"}
              </button>
            </form>
            <div className="progress-wrap" aria-live="polite">
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${sibecProgress}%` }} />
              </div>
              <small>{sibecUploading ? `Enviando: ${sibecProgress}%` : "Aguardando envio"}</small>
            </div>
            {sibecStatus && <p className={sibecStatus.includes("sucesso") ? "status-ok" : "error"}>{sibecStatus}</p>}
          </section>
        )}
      </main>

      <section className="ingestao-history" aria-labelledby="ingestao-history-title">
        <div className="ingestao-history-head">
          <div>
            <h2 id="ingestao-history-title">Última ingestão por base</h2>
            <p className="ingestao-history-sub">
              Uma linha por tabela RAW (ex.: todas as cargas SIBEC manutenções aparecem como a execução mais recente em <code>raw.sibec__manutencoes</code>).
            </p>
          </div>
          <button type="button" onClick={() => loadRuns()} disabled={runsLoading}>
            {runsLoading ? "Atualizando…" : "Atualizar lista"}
          </button>
        </div>
        {runsError && <p className="error">{runsError}</p>}
        {!runsError && runs.length === 0 && !runsLoading && (
          <p className="ingestao-desc" style={{ margin: 0 }}>
            Nenhuma execução registrada ainda.
          </p>
        )}
        {!runsError && runsByBase.length > 0 && (
          <div className="runs-table-wrap">
            <table className="runs-table">
              <thead>
                <tr>
                  <th>Última execução</th>
                  <th>Fonte</th>
                  <th>Dataset</th>
                  <th>Tabela RAW</th>
                  <th>Estratégia</th>
                  <th>Linhas</th>
                  <th>Status</th>
                  <th>Arquivo</th>
                  <th>Usuário</th>
                </tr>
              </thead>
              <tbody>
                {runsByBase.map((run) => (
                  <tr key={run.id}>
                    <td>{formatRunDate(run.finished_at || run.created_at)}</td>
                    <td>{run.source}</td>
                    <td>{run.dataset}</td>
                    <td className="cell-mono">raw.{run.target_table}</td>
                    <td>{run.strategy}</td>
                    <td>{run.row_count}</td>
                    <td className={run.status === "success" ? "cell-ok" : "cell-fail"}>{run.status}</td>
                    <td className="cell-mono" title={run.file_name}>
                      {run.file_name.length > 28 ? `${run.file_name.slice(0, 28)}…` : run.file_name}
                    </td>
                    <td>{run.created_by_email}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
