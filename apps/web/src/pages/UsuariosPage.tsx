import { type Dispatch, type FormEvent, type SetStateAction } from "react";
import { Navigate } from "react-router-dom";

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

type Props = {
  me: UserMe | null;
  users: UserMe[];
  userError: string;
  newUser: NewUserPayload;
  setNewUser: Dispatch<SetStateAction<NewUserPayload>>;
  onCreateUser: (e: FormEvent<HTMLFormElement>) => void;
};

export default function UsuariosPage({ me, users, userError, newUser, setNewUser, onCreateUser }: Props) {
  if (me?.role !== "superadmin") {
    return <Navigate to="/" replace />;
  }

  return (
    <section className="usuarios-page">
      <div className="usuarios-head">
        <h1>Usuários</h1>
        <p>Criação de contas e perfis de acesso ao VigSocial.</p>
      </div>
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
    </section>
  );
}
