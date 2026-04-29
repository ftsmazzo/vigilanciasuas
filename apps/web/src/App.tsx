import { useEffect, useState } from "react";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";
const TOKEN_KEY = "vigsocial_token";

type HealthResponse = {
  status: string;
};

type LoginResponse = {
  access_token: string;
  role: string;
  name: string;
};

type UserMe = {
  id: number;
  name: string;
  email: string;
  role: string;
};

type NewUserPayload = {
  name: string;
  email: string;
  password: string;
  role: string;
};

export default function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [token, setToken] = useState<string>(localStorage.getItem(TOKEN_KEY) || "");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [me, setMe] = useState<UserMe | null>(null);
  const [loadingMe, setLoadingMe] = useState(false);
  const [users, setUsers] = useState<UserMe[]>([]);
  const [userError, setUserError] = useState("");
  const [newUser, setNewUser] = useState<NewUserPayload>({
    name: "",
    email: "",
    password: "",
    role: "tecnico",
  });
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadStrategy, setUploadStrategy] = useState("replace");
  const [uploadStatus, setUploadStatus] = useState("");
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploading, setUploading] = useState(false);

  useEffect(() => {
    fetch(`${API_URL}/health`)
      .then((res) => res.json())
      .then((data) => setHealth(data))
      .catch(() => setHealth(null));
  }, []);

  useEffect(() => {
    if (!token) {
      setMe(null);
      return;
    }

    setLoadingMe(true);
    fetch(`${API_URL}/api/v1/users/me`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })
      .then(async (res) => {
        if (!res.ok) {
          throw new Error("Falha ao buscar usuário");
        }
        return res.json();
      })
      .then((data: UserMe) => setMe(data))
      .catch(() => {
        localStorage.removeItem(TOKEN_KEY);
        setToken("");
        setAuthError("Sessão inválida. Faça login novamente.");
      })
      .finally(() => setLoadingMe(false));
  }, [token]);

  useEffect(() => {
    if (!token || me?.role !== "superadmin") {
      setUsers([]);
      return;
    }

    fetch(`${API_URL}/api/v1/users`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })
      .then(async (res) => {
        if (!res.ok) throw new Error("Falha ao listar usuários");
        return res.json();
      })
      .then((data: UserMe[]) => setUsers(data))
      .catch(() => setUserError("Não foi possível carregar usuários."));
  }, [token, me?.role]);

  async function handleLogin(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAuthError("");

    try {
      const response = await fetch(`${API_URL}/api/v1/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ email, password }),
      });

      if (!response.ok) {
        throw new Error("Credenciais inválidas");
      }

      const data: LoginResponse = await response.json();
      localStorage.setItem(TOKEN_KEY, data.access_token);
      setToken(data.access_token);
      setPassword("");
    } catch {
      setAuthError("Login inválido. Verifique email e senha.");
    }
  }

  function handleLogout() {
    localStorage.removeItem(TOKEN_KEY);
    setToken("");
    setMe(null);
  }

  async function handleCreateUser(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setUserError("");

    try {
      const response = await fetch(`${API_URL}/api/v1/users`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(newUser),
      });

      if (!response.ok) {
        throw new Error("Erro ao criar usuário");
      }

      const created: UserMe = await response.json();
      setUsers((prev) => [created, ...prev]);
      setNewUser({ name: "", email: "", password: "", role: "tecnico" });
    } catch {
      setUserError("Falha ao criar usuário. Verifique os dados.");
    }
  }

  async function handleUpload(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setUploadStatus("");
    setUploadProgress(0);

    if (!uploadFile) {
      setUploadStatus("Selecione um arquivo CSV ou XLSX.");
      return;
    }

    const formData = new FormData();
    formData.append("file", uploadFile);
    formData.append("source", "cecad");
    formData.append("dataset", "cadu");
    formData.append("strategy", uploadStrategy);
    formData.append("csv_delimiter", ";");

    setUploading(true);
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API_URL}/api/v1/ingestion/import`);
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);

    xhr.upload.onprogress = (progressEvent) => {
      if (!progressEvent.lengthComputable) {
        return;
      }
      const percentage = Math.round((progressEvent.loaded / progressEvent.total) * 100);
      setUploadProgress(percentage);
    };

    xhr.onload = () => {
      setUploading(false);
      try {
        const responseBody = xhr.responseText ? JSON.parse(xhr.responseText) : {};
        if (xhr.status >= 200 && xhr.status < 300) {
          setUploadProgress(100);
          setUploadStatus(
            `Ingestão CADU concluída em raw.${responseBody.target_table} | linhas: ${responseBody.row_count} | estratégia: ${responseBody.strategy}`
          );
          setUploadFile(null);
          return;
        }
        setUploadStatus(responseBody.detail || "Falha na ingestão. Confirme formato, dados e sessão.");
      } catch {
        setUploadStatus("Falha na ingestão. Tente novamente.");
      }
    };

    xhr.onerror = () => {
      setUploading(false);
      setUploadStatus("Erro de rede durante a ingestão.");
    };

    xhr.send(formData);
  }

  return (
    <main className="page">
      <header className="hero">
        <h1>VigSocial</h1>
        <p>Painel inicial da Vigilância Socioassistencial</p>
      </header>

      {!token ? (
        <section className="auth-card">
          <h2>Entrar</h2>
          <form onSubmit={handleLogin} className="auth-form">
            <label>
              Email
              <input
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                required
              />
            </label>
            <label>
              Senha
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
              />
            </label>
            <button type="submit">Entrar</button>
          </form>
          {authError && <p className="error">{authError}</p>}
        </section>
      ) : (
        <section className="session-bar">
          <div>
            {loadingMe ? "Carregando sessão..." : `Logado como ${me?.name} (${me?.role})`}
          </div>
          <button type="button" onClick={handleLogout}>
            Sair
          </button>
        </section>
      )}

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

      {token && (
        <section className="auth-card">
          <h2>CADU - Cadastro Único (RAW)</h2>
          <form onSubmit={handleUpload} className="auth-form">
            <label>
              Estratégia
              <select
                value={uploadStrategy}
                onChange={(event) => setUploadStrategy(event.target.value)}
                disabled={uploading}
              >
                <option value="replace">replace (substituir tabela)</option>
                <option value="append">append (agregar linhas)</option>
              </select>
            </label>
            <label>
              Arquivo (CSV/XLSX)
              <input
                type="file"
                accept=".csv,.xlsx"
                onChange={(event) => setUploadFile(event.target.files?.[0] || null)}
                disabled={uploading}
                required
              />
            </label>
            <button type="submit" disabled={uploading}>
              {uploading ? "Processando..." : "Processar CADU para RAW"}
            </button>
          </form>
          <div className="progress-wrap" aria-live="polite">
            <div className="progress-track">
              <div className="progress-fill" style={{ width: `${uploadProgress}%` }} />
            </div>
            <small>{uploading ? `Enviando arquivo: ${uploadProgress}%` : "Aguardando envio"}</small>
          </div>
          {uploadStatus && <p>{uploadStatus}</p>}
        </section>
      )}

      {me?.role === "superadmin" && (
        <section className="auth-card">
          <h2>Gestão de Usuários</h2>
          <form onSubmit={handleCreateUser} className="auth-form">
            <label>
              Nome
              <input
                type="text"
                value={newUser.name}
                onChange={(event) => setNewUser((prev) => ({ ...prev, name: event.target.value }))}
                required
              />
            </label>
            <label>
              Email
              <input
                type="email"
                value={newUser.email}
                onChange={(event) => setNewUser((prev) => ({ ...prev, email: event.target.value }))}
                required
              />
            </label>
            <label>
              Senha
              <input
                type="password"
                value={newUser.password}
                onChange={(event) => setNewUser((prev) => ({ ...prev, password: event.target.value }))}
                required
              />
            </label>
            <label>
              Perfil
              <select
                value={newUser.role}
                onChange={(event) => setNewUser((prev) => ({ ...prev, role: event.target.value }))}
              >
                <option value="gestor">Gestor</option>
                <option value="admin_local">Admin Local</option>
                <option value="tecnico">Técnico</option>
                <option value="consultivo">Consultivo</option>
              </select>
            </label>
            <button type="submit">Criar usuário</button>
          </form>
          {userError && <p className="error">{userError}</p>}
          <div className="users-list">
            {users.map((user) => (
              <div className="user-row" key={user.id}>
                <strong>{user.name}</strong>
                <span>{user.email}</span>
                <span>{user.role}</span>
              </div>
            ))}
          </div>
        </section>
      )}
    </main>
  );
}
