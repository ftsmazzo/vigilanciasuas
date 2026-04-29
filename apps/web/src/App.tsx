import { useEffect, useState } from "react";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

type HealthResponse = {
  status: string;
};

export default function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    fetch(`${API_URL}/health`)
      .then((res) => res.json())
      .then((data) => setHealth(data))
      .catch(() => setHealth(null));
  }, []);

  return (
    <main className="page">
      <header className="hero">
        <h1>VigSocial</h1>
        <p>Painel inicial da Vigilância Socioassistencial</p>
      </header>

      <section className="cards">
        <article className="card">
          <h3>Usuários e Perfis</h3>
          <p>SuperAdmin, Gestor, Admin Local, Técnico e Consultivo</p>
        </article>
        <article className="card">
          <h3>Ingestão de Dados</h3>
          <p>Pipeline planejada para CSV/XLSX em tabelas RAW</p>
        </article>
        <article className="card">
          <h3>Status da API</h3>
          <p>{health?.status === "ok" ? "Online" : "Aguardando conexão"}</p>
        </article>
      </section>
    </main>
  );
}
