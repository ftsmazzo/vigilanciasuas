import { useEffect, useState, type Dispatch, type FormEvent, type ReactNode, type SetStateAction } from "react";
import { BrowserRouter, Navigate, NavLink, Route, Routes } from "react-router-dom";
import IngestaoPage from "./pages/IngestaoPage";

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

function AppShell({
  token,
  loadingMe,
  me,
  onLogout,
  children,
}: {
  token: string;
  loadingMe: boolean;
  me: UserMe | null;
  onLogout: () => void;
  children: ReactNode;
}) {
  return (
    <>
      <header className="hero">
        <h1>VigSocial</h1>
        <p>Painel inicial da Vigilância Socioassistencial</p>
      </header>

      <section className="session-bar">
        <div>
          {loadingMe ? "Carregando sessão…" : `Logado como ${me?.name} (${me?.role})`}
        </div>
        <div className="session-actions">
          <nav className="main-nav" aria-label="Principal">
            <NavLink to="/" end className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
              Início
            </NavLink>
            <NavLink to="/ingestao" className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
              Ingestão de dados
            </NavLink>
          </nav>
          <button type="button" onClick={onLogout}>
            Sair
          </button>
        </div>
      </section>

      {children}
    </>
  );
}

function DashboardHome({
  health,
  me,
  users,
  userError,
  newUser,
  setNewUser,
  onCreateUser,
}: {
  health: HealthResponse | null;
  me: UserMe | null;
  users: UserMe[];
  userError: string;
  newUser: NewUserPayload;
  setNewUser: Dispatch<SetStateAction<NewUserPayload>>;
  onCreateUser: (e: FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <>
      <section className="cards">
        <article className="card">
          <h3>Usuários e perfis</h3>
          <p>SuperAdmin, Gestor, Admin Local, Técnico e Consultivo</p>
        </article>
        <article className="card card-link">
          <h3>Ingestão de dados</h3>
          <p>Envio de CSV/XLSX para tabelas RAW (CADU, Bolsa Família, BPC, SIBEC).</p>
          <NavLink to="/ingestao" className="card-cta">
            Abrir página de ingestão →
          </NavLink>
        </article>
        <article className="card">
          <h3>Status da API</h3>
          <p>{health?.status === "ok" ? "Online" : "Aguardando conexão"}</p>
        </article>
      </section>

      {me?.role === "superadmin" && (
        <section className="auth-card">
          <h2>Gestão de usuários</h2>
          <form onSubmit={onCreateUser} className="auth-form">
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
    </>
  );
}

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

  async function handleLogin(event: FormEvent<HTMLFormElement>) {
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

  async function handleCreateUser(event: FormEvent<HTMLFormElement>) {
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

  return (
    <BrowserRouter>
      <main className="page">
        <Routes>
          <Route
            path="/"
            element={
              !token ? (
                <>
                  <header className="hero">
                    <h1>VigSocial</h1>
                    <p>Painel inicial da Vigilância Socioassistencial</p>
                  </header>
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
                  <section className="cards">
                    <article className="card">
                      <h3>Vigilância socioassistencial</h3>
                      <p>Faça login para acessar o painel e a ingestão de dados.</p>
                    </article>
                  </section>
                </>
              ) : (
                <AppShell token={token} loadingMe={loadingMe} me={me} onLogout={handleLogout}>
                  <DashboardHome
                    health={health}
                    me={me}
                    users={users}
                    userError={userError}
                    newUser={newUser}
                    setNewUser={setNewUser}
                    onCreateUser={handleCreateUser}
                  />
                </AppShell>
              )
            }
          />
          <Route
            path="/ingestao"
            element={
              token ? (
                <AppShell token={token} loadingMe={loadingMe} me={me} onLogout={handleLogout}>
                  <IngestaoPage token={token} />
                </AppShell>
              ) : (
                <Navigate to="/" replace />
              )
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}
