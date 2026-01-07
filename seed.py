import asyncio
from sqlalchemy import delete, select

# Utiliser les bons chemins d'importation relatifs au projet
from app.db import AsyncSessionLocal, engine
from app.models import Base, User, Role, Branch # Importer aussi Branch
from app.auth import hash_password # Utiliser hash_password

async def seed():
    """Crée les tables et ajoute les données initiales (magasins, utilisateurs)."""
    print("Début du script de seeding...")
    async with engine.begin() as conn:
        print("Création/Vérification des tables...")
        # Supprimer toutes les tables (optionnel, pour repartir de zéro)
        # ATTENTION: Supprime TOUTES les données existantes
        # await conn.run_sync(Base.metadata.drop_all)
        # print("Anciennes tables supprimées.")
        await conn.run_sync(Base.metadata.create_all)
        print("Tables créées/vérifiées.")

    async with AsyncSessionLocal() as session:
        # Vérifier si les magasins existent déjà pour éviter les doublons
        res_branches = await session.execute(select(Branch).limit(1))
        if res_branches.scalar_one_or_none() is None:
            print("Aucun magasin trouvé, création des magasins initiaux...")
            # Créer les magasins (Branches) en français
            branch_ariana = Branch(name="Magasin Ariana", city="Ariana")
            branch_nabeul = Branch(name="Magasin Nabeul", city="Nabeul")
            session.add_all([branch_ariana, branch_nabeul])
            await session.flush() # Important pour obtenir les IDs générés
            print(f"Magasins créés: '{branch_ariana.name}' (ID={branch_ariana.id}), '{branch_nabeul.name}' (ID={branch_nabeul.id})")
        else:
            print("Magasins déjà présents, récupération des IDs...")
            # Si les magasins existent, récupérer leurs IDs pour les assigner aux managers
            res_ariana = await session.execute(select(Branch).where(Branch.name == "Magasin Ariana"))
            branch_ariana = res_ariana.scalar_one_or_none()
            res_nabeul = await session.execute(select(Branch).where(Branch.name == "Magasin Nabeul"))
            branch_nabeul = res_nabeul.scalar_one_or_none()

            if not branch_ariana or not branch_nabeul:
                print("ERREUR: Impossible de trouver les magasins 'Magasin Ariana' ou 'Magasin Nabeul'. Stoppé.")
                return # Arrêter si on ne trouve pas les magasins attendus

        # Vérifier si l'admin existe déjà
        res_admin = await session.execute(select(User).where(User.email == "zaher@local"))
        if res_admin.scalar_one_or_none() is None:
            print("Admin 'zaher@local' non trouvé, création des utilisateurs initiaux...")
            # Supprimer les anciens utilisateurs si l'admin n'existe pas (pour être sûr)
            print("Suppression des anciens utilisateurs (si existants)...")
            await session.execute(delete(User))
            await session.flush()

            # Créer les utilisateurs (Admin/Managers)
            users_to_create = [
                User(
                    email="zaher@local",
                    full_name="BK ZAHER",
                    role=Role.admin, # Rôle admin
                    hashed_password=hash_password("5"), # Utiliser la bonne fonction
                    is_active=True,
                    branch_id=None # Admin n'est pas lié à un magasin
                ),
            ]
            session.add_all(users_to_create)
            await session.commit()
            print(f"✅ {len(users_to_create)} utilisateurs créés avec succès !")
        else:
            print("Utilisateur admin 'zaher@local' déjà présent. Seeding des utilisateurs ignoré.")

        print("Script de seeding terminé.")

if __name__ == "__main__":
    # Ce script doit être exécuté depuis le dossier racine (hr-sync)
    # avec la commande : python seed.py
    # Assurez-vous que les variables d'environnement (DATABASE_URL) sont définies.
    print("Lancement de la fonction seed asynchrone...")
    asyncio.run(seed())
    print("Exécution terminée.")
