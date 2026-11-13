# ms-notificaciones

Bandeja de avisos por usuario y tenant + línea de tiempo de eventos. Consume los 5 eventos con idempotencia por `event_id`.

- **Tablas DynamoDB** (PK `tenant_id`, solo `Query`): `sp_notificaciones`
- **Publica:** —
- **Consume:** NotaPublicada, EntregaRegistrada, ModuloPublicado, AlumnoEnRiesgo, QuizCompletado (cola sp-notificaciones + DLQ)
- **Seguridad:** Lambda Authorizer de `ms-auth` en todas las rutas; el tenant sale del token, nunca del cliente.

## Endpoints
| Método | Ruta | Acceso | Handler |
|---|---|---|---|
| GET | `/notificaciones` | token | `handler.listar_notificaciones` |
| POST | `/notificaciones/leida` | token | `handler.marcar_leida` |
| POST | `/notificaciones/leida-todas` | token | `handler.marcar_todas` |
| GET | `/notificaciones/traza` | token | `handler.traza_eventos` |

## Desplegar
```bash
# con credenciales del Learner Lab vigentes y tras desplegar infra-base
npx --yes serverless@3 deploy --stage dev
```
Requiere los stacks sp-infra-dev y ms-auth-dev ya desplegados
