"""Gestion administrative des contrats de maintenance.

Workflow à action : ``creer``, ``lier_marche``, ``avenant``, ``maj_parc``, ``cloturer`` et
``consulter``. Le registre est un fichier JSON versionné dans le dépôt GitHub ; il ne stocke
que les métadonnées et des références vers les documents sources (contrats signés, avenants,
pièces de marchés), jamais les documents eux-mêmes.
"""

import copy
import json
from datetime import timedelta
from typing import Any, Literal

import pydantic
from mistralai import workflows
from mistralai.workflows import Depends
from mistralai.workflows.plugins.mistralai.connectors import (
    ConnectorClient,
    ConnectorToolCallError,
    connector,
    uses_connectors,
)

GITHUB_CONNECTOR_NAME = "github_app"
DEFAULT_OWNER = "alebreton-CHSN"
DEFAULT_REPO = "Workflow-Contrats-Maintenance"
DEFAULT_REGISTRE_PATH = "registre/registre.json"
DEFAULT_BRANCHE = "main"

STATUT_ACTIF = "actif"
STATUT_CLOTURE = "cloture"
STATUT_AVENANT_APPLIQUE = "applique"

github = connector(GITHUB_CONNECTOR_NAME)

TypeAvenant = Literal[
    "prolongation", "revision_montant", "modification_perimetre", "autre"
]
OperationParc = Literal["ajouter", "retirer"]
MotifCloture = Literal["echeance", "resiliation", "reconduction", "autre"]

REGISTRE_INITIAL: dict[str, Any] = {"contrats": [], "marches": []}


class ErreurGestion(Exception):
    """Règle de gestion violée : l'action est refusée avec un message explicite."""


class Marche(pydantic.BaseModel):
    reference_marche: str
    objet: str | None = None
    montant: float | None = None
    doc_reference: str | None = None


class ItemParc(pydantic.BaseModel):
    designation: str
    site: str | None = None
    quantite: int = 1


class Avenant(pydantic.BaseModel):
    reference: str
    type: TypeAvenant
    date_effet: str
    nouvelle_date_fin: str | None = None
    nouveau_montant: float | None = None
    commentaire: str | None = None
    doc_reference: str | None = None


class ContratAction(pydantic.BaseModel):
    """Entrée unique du workflow : une action et ses paramètres."""

    model_config = pydantic.ConfigDict(extra="forbid")

    action: Literal[
        "creer", "lier_marche", "avenant", "maj_parc", "cloturer", "consulter"
    ]
    owner: str = DEFAULT_OWNER
    repo: str = DEFAULT_REPO
    chemin_registre: str = DEFAULT_REGISTRE_PATH
    branche: str = DEFAULT_BRANCHE
    dry_run: bool = False

    reference: str | None = None
    titre: str | None = None
    fournisseur: str | None = None
    date_debut: str | None = None
    date_fin: str | None = None
    montant: float | None = None
    description: str | None = None
    doc_reference: str | None = None

    marche: Marche | None = None
    avenant: Avenant | None = None
    operation_parc: OperationParc | None = None
    items_parc: list[ItemParc] = pydantic.Field(default_factory=list)
    motif: MotifCloture | None = None
    commentaire: str | None = None

    @pydantic.model_validator(mode="after")
    def _valider_champs_par_action(self) -> "ContratAction":
        manquants: list[str] = []
        if self.action == "creer":
            manquants = [
                c
                for c in ("reference", "fournisseur", "date_debut", "date_fin")
                if getattr(self, c) is None
            ]
        else:
            if self.reference is None:
                manquants.append("reference")
            if self.action == "lier_marche" and self.marche is None:
                manquants.append("marche")
            if self.action == "avenant" and self.avenant is None:
                manquants.append("avenant")
            if self.action == "maj_parc":
                if self.operation_parc is None:
                    manquants.append("operation_parc")
                if not self.items_parc:
                    manquants.append("items_parc")
            if self.action == "cloturer" and self.motif is None:
                manquants.append("motif")
        if manquants:
            raise ValueError(
                f"Champs requis manquants pour l'action '{self.action}' : {', '.join(manquants)}"
            )
        return self


class ResultatContrat(pydantic.BaseModel):
    action: str
    statut: Literal["succes", "erreur"]
    message: str
    dry_run: bool = False
    contrat: dict[str, Any] | None = None
    commit: str | None = None


class RegistreLu(pydantic.BaseModel):
    registre: dict[str, Any]
    sha: str | None = None


# --- Activités d'accès au registre (I/O via connecteur GitHub) -------------------------------


def _blocs_texte(resultat: Any) -> list[str]:
    contenu = resultat.get("content") if isinstance(resultat, dict) else None
    textes: list[str] = []
    for bloc in contenu or []:
        if isinstance(bloc, dict) and bloc.get("type") == "text":
            textes.append(str(bloc.get("text", "")))
    return textes


def _objets_json(resultat: Any) -> list[dict[str, Any]]:
    objets: list[dict[str, Any]] = []
    for texte in _blocs_texte(resultat):
        st = texte.strip()
        if st.startswith(("{", "[")):
            try:
                charge = json.loads(st)
            except json.JSONDecodeError:
                continue
            if isinstance(charge, dict):
                objets.append(charge)
    if isinstance(resultat, dict) and isinstance(
        resultat.get("structuredContent"), dict
    ):
        objets.append(resultat["structuredContent"])
    return objets


def _fichier_absent(erreur: ConnectorToolCallError) -> bool:
    reponse = erreur.response if isinstance(erreur.response, dict) else {}
    texte = " ".join(_blocs_texte(reponse)).lower()
    marqueurs = ("not found", "no file", "does not exist", "404", "introuvable")
    return any(marqueur in texte for marqueur in marqueurs)


async def _lire_registre_reel(
    client: ConnectorClient, owner: str, repo: str, chemin: str, branche: str
) -> RegistreLu:
    try:
        resultat = await client.call_tool(
            "get_file_contents",
            {"owner": owner, "repo": repo, "path": chemin, "ref": branche},
        )
    except ConnectorToolCallError as erreur:
        if _fichier_absent(erreur):
            return RegistreLu(registre=copy.deepcopy(REGISTRE_INITIAL), sha=None)
        raise
    objets = _objets_json(resultat)
    sha = next((str(o["sha"]) for o in objets if o.get("sha")), None)
    for objet in objets:
        if isinstance(objet.get("contrats"), list) and isinstance(
            objet.get("marches"), list
        ):
            return RegistreLu(registre=objet, sha=sha)
    return RegistreLu(registre=copy.deepcopy(REGISTRE_INITIAL), sha=sha)


async def _ecrire_registre_reel(
    client: ConnectorClient,
    owner: str,
    repo: str,
    chemin: str,
    branche: str,
    registre: dict[str, Any],
    sha: str | None,
    message: str,
) -> str:
    arguments: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "path": chemin,
        "branch": branche,
        "content": json.dumps(registre, ensure_ascii=False, indent=2) + "\n",
        "message": message,
    }
    if sha:
        arguments["sha"] = sha
    resultat = await client.call_tool("create_or_update_file", arguments)
    for objet in _objets_json(resultat):
        commit = objet.get("commit")
        if isinstance(commit, dict) and commit.get("sha"):
            return str(commit["sha"])
        if objet.get("sha") and "commit" in objet:
            return str(objet["sha"])
    return ""


@workflows.activity(name="lire-registre", start_to_close_timeout=timedelta(seconds=60))
async def lire_registre(
    owner: str,
    repo: str,
    chemin: str,
    branche: str,
    client: ConnectorClient = Depends(github),  # noqa: B008 - motif imposé par le SDK
) -> RegistreLu:
    return await _lire_registre_reel(client, owner, repo, chemin, branche)


@workflows.activity(
    name="ecrire-registre",
    start_to_close_timeout=timedelta(seconds=60),
    retry_policy_max_attempts=1,
)
async def ecrire_registre(
    owner: str,
    repo: str,
    chemin: str,
    branche: str,
    registre: dict[str, Any],
    sha: str | None,
    message: str,
    client: ConnectorClient = Depends(github),  # noqa: B008 - motif imposé par le SDK
) -> str:
    return await _ecrire_registre_reel(
        client, owner, repo, chemin, branche, registre, sha, message
    )


# --- Logique de gestion (déterministe, sans I/O) ---------------------------------------------


def _normaliser_registre(registre: dict[str, Any]) -> dict[str, Any]:
    normalise = dict(registre or {})
    normalise.setdefault("contrats", [])
    normalise.setdefault("marches", [])
    return normalise


def _trouver_contrat(registre: dict[str, Any], reference: str) -> dict[str, Any] | None:
    return next(
        (c for c in registre["contrats"] if c.get("reference") == reference), None
    )


def _trouver_marche(
    registre: dict[str, Any], reference_marche: str
) -> dict[str, Any] | None:
    return next(
        (
            m
            for m in registre["marches"]
            if m.get("reference_marche") == reference_marche
        ),
        None,
    )


def _ajouter_historique(
    entite: dict[str, Any], action: str, detail: str, maintenant: str
) -> None:
    entite.setdefault("historique", []).append(
        {"date": maintenant, "action": action, "detail": detail}
    )


def _exiger_contrat_actif(registre: dict[str, Any], reference: str) -> dict[str, Any]:
    contrat = _trouver_contrat(registre, reference)
    if contrat is None:
        raise ErreurGestion(f"Contrat '{reference}' introuvable dans le registre")
    if contrat.get("statut") == STATUT_CLOTURE:
        raise ErreurGestion(f"Contrat '{reference}' est clôturé : action impossible")
    return contrat


def _appliquer_action(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    if donnees.action == "creer":
        return _creer_contrat(donnees, registre, maintenant)
    if donnees.action == "lier_marche":
        return _lier_marche(donnees, registre, maintenant)
    if donnees.action == "avenant":
        return _ajouter_avenant(donnees, registre, maintenant)
    if donnees.action == "maj_parc":
        return _maj_parc(donnees, registre, maintenant)
    if donnees.action == "cloturer":
        return _cloturer_contrat(donnees, registre, maintenant)
    return _consulter_contrat(donnees, registre)


def _creer_contrat(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    if _trouver_contrat(registre, donnees.reference) is not None:
        raise ErreurGestion(
            f"Un contrat avec la référence '{donnees.reference}' existe déjà"
        )
    contrat: dict[str, Any] = {
        "reference": donnees.reference,
        "titre": donnees.titre,
        "fournisseur": donnees.fournisseur,
        "date_debut": donnees.date_debut,
        "date_fin": donnees.date_fin,
        "montant": donnees.montant,
        "description": donnees.description,
        "doc_reference": donnees.doc_reference,
        "statut": STATUT_ACTIF,
        "marches": [],
        "parc": [],
        "avenants": [],
        "historique": [],
    }
    registre["contrats"].append(contrat)
    _ajouter_historique(
        contrat, "creer", "Création du contrat de maintenance", maintenant
    )
    return contrat


def _lier_marche(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    contrat = _exiger_contrat_actif(registre, donnees.reference)
    donnees_marche = donnees.marche
    marche = _trouver_marche(registre, donnees_marche.reference_marche)
    if marche is None:
        marche = {
            "reference_marche": donnees_marche.reference_marche,
            "objet": donnees_marche.objet,
            "montant": donnees_marche.montant,
            "doc_reference": donnees_marche.doc_reference,
            "contrats": [],
            "historique": [],
        }
        registre["marches"].append(marche)
    if contrat["reference"] in marche["contrats"]:
        raise ErreurGestion(
            f"Le contrat '{contrat['reference']}' est déjà lié au marché '{marche['reference_marche']}'"
        )
    marche["contrats"].append(contrat["reference"])
    contrat["marches"].append(marche["reference_marche"])
    detail = f"Lien avec le marché '{marche['reference_marche']}'"
    _ajouter_historique(contrat, "lier_marche", detail, maintenant)
    _ajouter_historique(
        marche, "lier_contrat", f"Lien du contrat '{contrat['reference']}'", maintenant
    )
    return contrat


def _ajouter_avenant(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    contrat = _exiger_contrat_actif(registre, donnees.reference)
    avenant = donnees.avenant
    if any(a.get("reference") == avenant.reference for a in contrat["avenants"]):
        raise ErreurGestion(
            f"L'avenant '{avenant.reference}' existe déjà sur le contrat '{contrat['reference']}'"
        )
    effets: dict[str, Any] = {}
    if avenant.nouvelle_date_fin is not None:
        effets["nouvelle_date_fin"] = avenant.nouvelle_date_fin
    if avenant.nouveau_montant is not None:
        effets["nouveau_montant"] = avenant.nouveau_montant
    enregistrement = {
        "reference": avenant.reference,
        "type": avenant.type,
        "date_effet": avenant.date_effet,
        "commentaire": avenant.commentaire,
        "doc_reference": avenant.doc_reference,
        "statut": STATUT_AVENANT_APPLIQUE,
        "date_creation": maintenant,
        "effets": effets,
    }
    if avenant.nouvelle_date_fin is not None:
        contrat["date_fin"] = avenant.nouvelle_date_fin
    if avenant.nouveau_montant is not None:
        contrat["montant"] = avenant.nouveau_montant
    contrat["avenants"].append(enregistrement)
    _ajouter_historique(
        contrat,
        "avenant",
        f"Avenant '{avenant.reference}' ({avenant.type}) appliqué"
        + (f" — effets : {', '.join(effets)}" if effets else ""),
        maintenant,
    )
    return contrat


def _cle_item_parc(designation: str | None, site: str | None) -> tuple[str, str]:
    return (designation or "").strip().lower(), (site or "").strip().lower()


def _maj_parc(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    contrat = _exiger_contrat_actif(registre, donnees.reference)
    parc = contrat.setdefault("parc", [])
    for item in donnees.items_parc:
        cle = _cle_item_parc(item.designation, item.site)
        existant = next(
            (
                p
                for p in parc
                if _cle_item_parc(p.get("designation"), p.get("site")) == cle
            ),
            None,
        )
        if donnees.operation_parc == "ajouter":
            if existant is not None:
                existant["quantite"] = item.quantite
            else:
                parc.append(
                    {
                        "designation": item.designation,
                        "site": item.site,
                        "quantite": item.quantite,
                    }
                )
        else:
            if existant is None:
                raise ErreurGestion(
                    f"Élément de parc '{item.designation}'"
                    + (f" sur le site '{item.site}'" if item.site else "")
                    + f" introuvable dans le parc du contrat '{contrat['reference']}'"
                )
            parc.remove(existant)
    detail = f"{donnees.operation_parc} de {len(donnees.items_parc)} élément(s) au parc concerné"
    _ajouter_historique(contrat, "maj_parc", detail, maintenant)
    return contrat


def _cloturer_contrat(
    donnees: ContratAction, registre: dict[str, Any], maintenant: str
) -> dict[str, Any]:
    contrat = _exiger_contrat_actif(registre, donnees.reference)
    en_cours = [
        a["reference"]
        for a in contrat.get("avenants", [])
        if a.get("statut") != STATUT_AVENANT_APPLIQUE
    ]
    if en_cours:
        raise ErreurGestion(
            f"Clôture refusée : avenants en cours sur le contrat '{contrat['reference']}' : {', '.join(en_cours)}"
        )
    contrat["statut"] = STATUT_CLOTURE
    contrat["date_cloture"] = maintenant
    contrat["motif_cloture"] = donnees.motif
    contrat["commentaire_cloture"] = donnees.commentaire
    _ajouter_historique(
        contrat, "cloturer", f"Clôture du contrat (motif : {donnees.motif})", maintenant
    )
    return contrat


def _consulter_contrat(
    donnees: ContratAction, registre: dict[str, Any]
) -> dict[str, Any]:
    contrat = _trouver_contrat(registre, donnees.reference)
    if contrat is None:
        raise ErreurGestion(
            f"Contrat '{donnees.reference}' introuvable dans le registre"
        )
    return contrat


# --- Workflow --------------------------------------------------------------------------------


@workflows.workflow.define(name="gestion-contrats-maintenance", on_behalf_of=True)
@uses_connectors(github)
class GestionContratsMaintenance:
    """Gestion administrative des contrats de maintenance (registre JSON versionné)."""

    @workflows.workflow.entrypoint
    async def run(self, donnees: ContratAction) -> ResultatContrat:
        maintenant = workflows.workflow.now().isoformat()
        lu = await lire_registre(
            owner=donnees.owner,
            repo=donnees.repo,
            chemin=donnees.chemin_registre,
            branche=donnees.branche,
        )
        registre = _normaliser_registre(lu.registre)
        try:
            contrat = _appliquer_action(donnees, registre, maintenant)
        except ErreurGestion as erreur:
            return ResultatContrat(
                action=donnees.action, statut="erreur", message=str(erreur)
            )
        if donnees.action == "consulter":
            return ResultatContrat(
                action=donnees.action,
                statut="succes",
                message="Consultation du contrat",
                contrat=contrat,
            )
        if donnees.dry_run:
            return ResultatContrat(
                action=donnees.action,
                statut="succes",
                message="Simulation effectuée : aucune écriture dans le registre",
                dry_run=True,
                contrat=contrat,
            )
        commit = await ecrire_registre(
            owner=donnees.owner,
            repo=donnees.repo,
            chemin=donnees.chemin_registre,
            branche=donnees.branche,
            registre=registre,
            sha=lu.sha,
            message=f"contrats-maintenance: {donnees.action} {donnees.reference}",
        )
        return ResultatContrat(
            action=donnees.action,
            statut="succes",
            message="Registre mis à jour",
            contrat=contrat,
            commit=commit,
        )
