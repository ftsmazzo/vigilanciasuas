# VigSocial - Arquitetura Inicial

Aplicacao web para Vigilancia Socioassistencial com frontend e backend separados.

## Stack inicial

- Frontend: React + Vite
- Backend: FastAPI
- Banco de dados: PostgreSQL
- Fila/cache: Redis
- Orquestracao local: Docker Compose

## Perfis de acesso (RBAC)

- `superadmin`
- `gestor`
- `admin_local`
- `tecnico`
- `consultivo`

## Bootstrap do SuperAdmin

No primeiro deploy, defina no EasyPanel:

- `BOOTSTRAP_SUPERADMIN_EMAIL`
- `BOOTSTRAP_SUPERADMIN_PASSWORD`
- `BOOTSTRAP_SUPERADMIN_NAME`

Ao iniciar a API, se esse email ainda nao existir no banco, o usuario `superadmin` e criado automaticamente.

## Estrutura do projeto

- `apps/api`: API FastAPI com login e gestao inicial de usuarios
- `apps/web`: dashboard inicial com cards modernos
- `DadosBrutos`: fontes iniciais de dados para ingestao

## Rodando local com Docker

1. Copie `.env.example` para `.env`
2. Execute:
   - `docker compose up --build`
3. URLs:
   - Frontend: `http://localhost:3000`
   - API: `http://localhost:8000`
   - Docs API: `http://localhost:8000/docs`

## Endpoints iniciais

- `GET /health`
- `POST /api/v1/auth/login`
- `GET /api/v1/users/me`
- `POST /api/v1/users` (apenas `superadmin`)

## Proximos passos

- Implementar migracoes versionadas (Alembic)
- Criar modulo de upload CSV/XLSX e jobs assicronos
- Criar tabelas RAW e mapeamento inicial de `DadosBrutos/CECAD/tudo.csv`
- Definir views analiticas e cruzamentos da dashboard
