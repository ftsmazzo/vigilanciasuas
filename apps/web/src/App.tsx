import { useEffect, useState, type Dispatch, type FormEvent, type ReactNode, type SetStateAction } from "react";
import { BrowserRouter, Navigate, NavLink, Route, Routes } from "react-router-dom";
import IngestaoPage from "./pages/IngestaoPage";
import PainelIndicadoresInicio from "./pages/PainelIndicadoresInicio";
import UsuariosPage from "./pages/UsuariosPage";
import VigilanciaPage from "./pages/VigilanciaPage";

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
            <NavLink to="/vigilancia" className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
              Dados vigilância
            </NavLink>
            {me?.role === "superadmin" && (
              <NavLink to="/usuarios" className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
                Usuários
              </NavLink>
            )}
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

function DashboardHome({ token }: { token: string }) {
  return <PainelIndicadoresInicio token={token} />;
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
                  <DashboardHome token={token} />
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
          <Route
            path="/usuarios"
            element={
              token ? (
                <AppShell token={token} loadingMe={loadingMe} me={me} onLogout={handleLogout}>
                  <UsuariosPage
                    me={me}
                    users={users}
                    userError={userError}
                    newUser={newUser}
                    setNewUser={setNewUser}
                    onCreateUser={handleCreateUser}
                  />
                </AppShell>
              ) : (
                <Navigate to="/" replace />
              )
            }
          />
          <Route
            path="/vigilancia"
            element={
              token ? (
                <AppShell token={token} loadingMe={loadingMe} me={me} onLogout={handleLogout}>
                  <VigilanciaPage token={token} />
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
