"""
Utilitaires de journalisation d'audit.
"""
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

# --- MODIFIÉ : Ajout de 'User' pour charger les noms ---
from .models import AuditLog, User
from .schemas import AuditOut


async def log(
    session: AsyncSession,
    actor_id: int,
    action: str,
    entity: str,
    entity_id: int | None,
    branch_id: int | None,
    details: str | None = None,
) -> None:
    """Enregistrer une entrée dans le journal d'audit."""
    session.add(
        AuditLog(
            actor_id=actor_id,
            action=action,
            entity=entity,
            entity_id=entity_id,
            branch_id=branch_id,
            details=details,
        )
    )
    await session.commit()


async def latest(
    session: AsyncSession,
    limit: int = 50,
    user_is_admin: bool = False,
    branch_id: int | None = None,
    entity_types: list[str] | None = None
) -> list[AuditOut]:
    """
    Retourne les entrées d'audit les plus récentes jusqu'à `limit`.
    Filtre par branch_id si l'utilisateur n'est pas admin.
    Filtre par entity_types si fourni (pour la page Paramètres).
    """
    
    # --- 1. Requête pour les journaux d'audit ---
    stmt = select(AuditLog).order_by(AuditLog.created_at.desc())

    if not user_is_admin and branch_id is not None:
        stmt = stmt.where(AuditLog.branch_id == branch_id)

    if entity_types:
        stmt = stmt.where(AuditLog.entity.in_(entity_types))

    stmt = stmt.limit(limit)
    res = await session.execute(stmt)
    logs = res.scalars().all() # Obtenir les objets AuditLog
    
    if not logs:
        return []

    # --- 2. Correction "Qui" (Demande 3) : Charger les acteurs manuellement ---
    
    # Trouver tous les IDs uniques des acteurs
    actor_ids = {log.actor_id for log in logs}
    
    # Requête pour trouver les utilisateurs correspondants
    user_stmt = select(User).where(User.id.in_(actor_ids))
    user_res = await session.execute(user_stmt)
    
    # Créer un dictionnaire (map) pour un accès facile : {id: full_name}
    actors_map = {user.id: user.full_name for user in user_res.scalars().all()}

    # --- 3. Créer la liste finale (AuditOut) ---
    # Convertir les modèles SQLAlchemy en modèles Pydantic (schemas)
    # et insérer manuellement le 'actor_full_name'
    
    output_logs = []
    for log in logs:
        # Valider le log avec le schéma
        log_out = AuditOut.model_validate(log)
        
        # Ajouter le nom complet de l'acteur depuis notre map
        # CELA NE FONCTIONNERA QUE SI VOUS MODIFIEZ app/schemas.py
        try:
            log_out.actor_full_name = actors_map.get(log.actor_id, "Utilisateur Inconnu")
        except ValueError:
            # Au cas où l'utilisateur n'a pas encore modifié schemas.py,
            # nous ignorons l'erreur pour ne pas crasher.
            pass 
        
        output_logs.append(log_out)

    return output_logs
