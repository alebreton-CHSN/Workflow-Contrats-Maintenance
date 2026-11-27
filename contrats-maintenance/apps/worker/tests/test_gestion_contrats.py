"""Tests du workflow gestion-contrats-maintenance avec registre simulé."""

import copy
from datetime import timedelta

import pytest
from mistralai.workflows.testing import create_test_worker
from worker.workflows import gestion_contrats
from worker.workflows.gestion_contrats import (
    GestionContratsMaintenance,
    RegistreLu,
    ecrire_registre,
    lire_registre,
)

WORKFLOW_TIMEOUT = timedelta(seconds=10)
TASK_QUEUE = "test-task-queue"


class FauxRegistre:
    """Remplace les appels connecteur GitHub par un registre en mémoire."""

    def __init__(self) -> None:
        self.registre: dict = {"contrats": [], "marches": []}
        self.commits: list[str] = []
        self.ecritures = 0

    async def lire(self, client, owner, repo, chemin, branche) -> RegistreLu:
        return RegistreLu(
            registre=copy.deepcopy(self.registre),
            sha="sha-existant" if self.ecritures else None,
        )

    async def ecrire(
        self, client, owner, repo, chemin, branche, registre, sha, message
    ) -> str:
        self.registre = copy.deepcopy(registre)
        self.commits.append(message)
        self.ecritures += 1
        return f"commit-{self.ecritures}"


@pytest.fixture()
def faux_registre(monkeypatch: pytest.MonkeyPatch) -> FauxRegistre:
    registre = FauxRegistre()

    async def lire(*args, **kwargs) -> RegistreLu:
        return await registre.lire(*args, **kwargs)

    async def ecrire(*args, **kwargs) -> str:
        return await registre.ecrire(*args, **kwargs)

    monkeypatch.setattr(gestion_contrats, "_lire_registre_reel", lire)
    monkeypatch.setattr(gestion_contrats, "_ecrire_registre_reel", ecrire)
    return registre


async def demarrer(temporal_env, entree: dict) -> dict:
    async with create_test_worker(
        temporal_env,
        workflows=[GestionContratsMaintenance],
        activities=[lire_registre, ecrire_registre],
    ):
        handle = await temporal_env.client.start_workflow(
            "gestion-contrats-maintenance",
            entree,
            id=f"test-{entree['action']}-{abs(hash(str(entree)))}",
            task_queue=TASK_QUEUE,
            execution_timeout=WORKFLOW_TIMEOUT,
        )
        import asyncio

        return await asyncio.wait_for(handle.result(), timeout=15)


CREATION = {
    "action": "creer",
    "reference": "C-2025-001",
    "titre": "Maintenance HVAC",
    "fournisseur": "ACME Services",
    "date_debut": "2025-01-01",
    "date_fin": "2026-01-01",
    "montant": 12000.0,
    "doc_reference": "https://ged.example/contrats/C-2025-001.pdf",
}


async def test_creation_de_contrat(temporal_env, faux_registre: FauxRegistre) -> None:
    resultat = await demarrer(temporal_env, dict(CREATION))
    assert resultat["statut"] == "succes"
    assert resultat["contrat"]["statut"] == "actif"
    assert faux_registre.registre["contrats"][0]["reference"] == "C-2025-001"
    assert resultat["commit"] == "commit-1"


async def test_creation_en_double_refusee(
    temporal_env, faux_registre: FauxRegistre
) -> None:
    await demarrer(temporal_env, dict(CREATION))
    resultat = await demarrer(temporal_env, dict(CREATION))
    assert resultat["statut"] == "erreur"
    assert "existe déjà" in resultat["message"]
    assert faux_registre.ecritures == 1


async def test_dry_run_n_ecrit_pas(temporal_env, faux_registre: FauxRegistre) -> None:
    entree = dict(CREATION, dry_run=True)
    resultat = await demarrer(temporal_env, entree)
    assert resultat["dry_run"] is True
    assert resultat["contrat"]["reference"] == "C-2025-001"
    assert faux_registre.ecritures == 0


async def test_cycle_complet_marche_avenant_parc_cloture(
    temporal_env, faux_registre: FauxRegistre
) -> None:
    await demarrer(temporal_env, dict(CREATION))

    lien = await demarrer(
        temporal_env,
        {
            "action": "lier_marche",
            "reference": "C-2025-001",
            "marche": {
                "reference_marche": "M-2025-07",
                "objet": "Maintenance des installations",
                "montant": 100000.0,
                "doc_reference": "https://ged.example/marches/M-2025-07.pdf",
            },
        },
    )
    assert lien["statut"] == "succes"
    assert lien["contrat"]["marches"] == ["M-2025-07"]
    assert faux_registre.registre["marches"][0]["contrats"] == ["C-2025-001"]

    avenant = await demarrer(
        temporal_env,
        {
            "action": "avenant",
            "reference": "C-2025-001",
            "avenant": {
                "reference": "AV-001",
                "type": "prolongation",
                "date_effet": "2026-01-02",
                "nouvelle_date_fin": "2027-01-01",
                "nouveau_montant": 15000.0,
                "doc_reference": "https://ged.example/avenants/AV-001.pdf",
            },
        },
    )
    assert avenant["statut"] == "succes"
    assert avenant["contrat"]["date_fin"] == "2027-01-01"
    assert avenant["contrat"]["montant"] == 15000.0

    maj = await demarrer(
        temporal_env,
        {
            "action": "maj_parc",
            "reference": "C-2025-001",
            "operation_parc": "ajouter",
            "items_parc": [
                {"designation": "Groupe froid GF-1", "site": "Lyon", "quantite": 2}
            ],
        },
    )
    assert maj["statut"] == "succes"
    assert maj["contrat"]["parc"][0]["designation"] == "Groupe froid GF-1"

    retrait = await demarrer(
        temporal_env,
        {
            "action": "maj_parc",
            "reference": "C-2025-001",
            "operation_parc": "retirer",
            "items_parc": [{"designation": "groupe froid gf-1", "site": "lyon"}],
        },
    )
    assert retrait["statut"] == "succes"
    assert retrait["contrat"]["parc"] == []

    cloture = await demarrer(
        temporal_env,
        {
            "action": "cloturer",
            "reference": "C-2025-001",
            "motif": "resiliation",
            "commentaire": "Fin anticipée",
        },
    )
    assert cloture["statut"] == "succes"
    assert cloture["contrat"]["statut"] == "cloture"
    assert cloture["contrat"]["motif_cloture"] == "resiliation"

    consult = await demarrer(
        temporal_env, {"action": "consulter", "reference": "C-2025-001"}
    )
    assert consult["statut"] == "succes"
    assert consult["contrat"]["date_fin"] == "2027-01-01"
    assert len(faux_registre.commits) == 6


async def test_action_sur_contrat_inconnu_refusee(
    temporal_env, faux_registre: FauxRegistre
) -> None:
    resultat = await demarrer(
        temporal_env, {"action": "consulter", "reference": "INCONNU"}
    )
    assert resultat["statut"] == "erreur"
    assert "introuvable" in resultat["message"]


async def test_maj_parc_retirer_inconnu_refuse(
    temporal_env, faux_registre: FauxRegistre
) -> None:
    await demarrer(temporal_env, dict(CREATION))
    resultat = await demarrer(
        temporal_env,
        {
            "action": "maj_parc",
            "reference": "C-2025-001",
            "operation_parc": "retirer",
            "items_parc": [{"designation": "Inexistant"}],
        },
    )
    assert resultat["statut"] == "erreur"
    assert "introuvable" in resultat["message"]


async def test_cloture_apres_prolongation_et_refus_action_sur_cloture(
    temporal_env, faux_registre
) -> None:
    await demarrer(temporal_env, dict(CREATION))
    await demarrer(
        temporal_env,
        {"action": "cloturer", "reference": "C-2025-001", "motif": "echeance"},
    )
    resultat = await demarrer(
        temporal_env,
        {
            "action": "lier_marche",
            "reference": "C-2025-001",
            "marche": {"reference_marche": "M-1"},
        },
    )
    assert resultat["statut"] == "erreur"
    assert "clôturé" in resultat["message"]
