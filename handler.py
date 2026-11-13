"""ms-notificaciones · bandeja de avisos.  CONSUMIDOR de todos los eventos de negocio.

Tabla:
  sp_notificaciones  PK tenant_id · SK usuario_ts ("A00001#2026-11-12T10:00:00Z#<event_id>")
                     GSI gsi_tiempo (tenant_id, creado_en)  -> "traza de eventos" del tenant

IDEMPOTENCIA: la clave incluye el event_id; si SQS entrega el mismo mensaje dos veces,
el segundo PutItem sobrescribe el mismo ítem (no hay duplicados).
"""
from boto3.dynamodb.conditions import Attr, Key

from common import (ApiError, body, consulta_paginada, contar, exigir_rol, log, procesar_lote,
                    protegido, query_params, query_todo, requerido, tabla, update_item,
                    verificar_tenant)

NOTIF = tabla("T_NOTIFICACIONES")


# ------------------------------------------------- consumidor (cola SQS)
def _destinos(msg):
    ev, d = msg["evento"], msg["detalle"]
    if ev == "NotaPublicada":
        return [(d["alumno_id"], "NOTA", f"Nueva nota en {d['titulo']}", f"Obtuviste {d['nota']} / 20.")]
    if ev == "EntregaRegistrada":
        tarde = " (fuera de plazo)" if d.get("es_tardia") else ""
        return [(d.get("docente_id"), "ENTREGA", f"Nueva entrega en {d['titulo']}",
                 f"{d['alumno_nombre']} entregó la tarea{tarde}.")]
    if ev == "ModuloPublicado":
        return [(a, "MODULO", f"Nuevo módulo en {d['curso_nombre']}", f"Se publicó «{d['titulo']}».")
                for a in d.get("alumnos", [])]
    if ev == "AlumnoEnRiesgo":
        txt = f"{d['alumno_nombre']} pasó a ROJO (puntaje {d['puntaje']}): " + "; ".join(d.get("motivos", []))
        return [(d.get("docente_id"), "RIESGO", "Alumno en riesgo académico", txt),
                (d["alumno_id"], "RIESGO", "Tu semáforo cambió a rojo",
                 "Revisa tus tareas pendientes y habla con tu docente.")]
    if ev == "QuizCompletado":
        return [(d["alumno_id"], "QUIZ", "Quiz completado", f"Acertaste {d['aciertos']} de {d['total']}.")]
    log("WARN", accion="evento_desconocido", evento=ev)
    return []


def _procesar(msg):
    tenant, destinos = msg["tenant_id"], [x for x in _destinos(msg) if x[0]]
    with NOTIF.batch_writer(overwrite_by_pkeys=["tenant_id", "usuario_ts"]) as bw:
        for user_id, tipo, titulo, texto in destinos:
            bw.put_item(Item={
                "tenant_id": tenant, "usuario_ts": f"{user_id}#{msg['ts']}#{msg['event_id']}",
                "user_id": user_id, "tipo": tipo, "titulo": titulo, "mensaje": texto,
                "leida": False, "evento": msg["evento"], "event_id": msg["event_id"],
                "origen": msg.get("origen", ""), "creado_en": msg["ts"]})
    log("INFO", accion="notificaciones_creadas", evento=msg["evento"], tenant_id=tenant, n=len(destinos))


def procesar_notificacion(event, context):
    return procesar_lote(event, _procesar)


# ---------------------------------------------------------------- API
def _bandeja_kw(ctx):
    return {"KeyConditionExpression": Key("tenant_id").eq(ctx["tenant_id"])
            & Key("usuario_ts").begins_with(ctx["user_id"] + "#"), "ScanIndexForward": False}


@protegido
def listar_notificaciones(event, ctx):
    qs = query_params(event)
    verificar_tenant(ctx, qs)
    kw = _bandeja_kw(ctx)  # SIEMPRE la bandeja del usuario del token: no se puede pedir la de otro
    if qs.get("solo_no_leidas") == "true":
        kw["FilterExpression"] = Attr("leida").eq(False)
    items, cursor = consulta_paginada(NOTIF, qs, kw, pk=("tenant_id", ctx["tenant_id"]))
    no_leidas = contar(NOTIF, FilterExpression=Attr("leida").eq(False), **_bandeja_kw(ctx))
    return {"notificaciones": items, "no_leidas": no_leidas, "cursor": cursor}


@protegido
def marcar_leida(event, ctx):
    d = body(event)
    verificar_tenant(ctx, d)
    nid = requerido(d, "id", 200)
    if not nid.startswith(ctx["user_id"] + "#"):
        raise ApiError(403, "NOTIFICACION_AJENA", "Esa notificación no es tuya")
    return update_item(NOTIF, {"tenant_id": ctx["tenant_id"], "usuario_ts": nid}, {"leida": True}, ["leida"])


@protegido
def marcar_todas(event, ctx):
    pendientes = query_todo(NOTIF, maximo=200, FilterExpression=Attr("leida").eq(False), **_bandeja_kw(ctx))
    for n in pendientes:
        update_item(NOTIF, {"tenant_id": ctx["tenant_id"], "usuario_ts": n["usuario_ts"]},
                    {"leida": True}, ["leida"])
    return {"marcadas": len(pendientes)}


@protegido
def traza_eventos(event, ctx):
    """Línea de tiempo de eventos del tenant (para mostrar el flujo asíncrono en vivo)."""
    exigir_rol(ctx, "docente", "admin")
    qs = query_params(event)
    kw = {"IndexName": "gsi_tiempo", "KeyConditionExpression": Key("tenant_id").eq(ctx["tenant_id"]),
          "ScanIndexForward": False}
    items, _ = consulta_paginada(NOTIF, {"limit": qs.get("limit", "15")}, kw)
    campos = ("evento", "tipo", "titulo", "mensaje", "origen", "event_id", "creado_en", "user_id")
    return {"eventos": [{k: i.get(k) for k in campos} for i in items]}
