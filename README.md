# Workflow — Gestion administrative des contrats de maintenance

Workflow Mistral (`gestion-contrats-maintenance`) piloté par **actions** : chaque exécution
applique une opération administrative sur les contrats de maintenance.

## Modèle de données

Le registre `registre/registre.json` (versionné dans ce dépôt) stocke **uniquement des
références vers les documents** (contrats signés, avenants, pièces de marchés) :

- **Contrat** : `reference`, `titre`, `fournisseur`, `date_debut`, `date_fin`, `montant`,
  `statut` (`actif`/`cloture`), `doc_reference`, `marches`, `parc`, `avenants`, `historique`.
- **Marché** : `reference_marche`, `objet`, `montant`, `doc_reference`, `contrats` liés.
- **Avenant** : `reference`, `type` (`prolongation`, `revision_montant`,
  `modification_perimetre`, `autre`), `date_effet`, `effets` appliqués, `doc_reference`.
- **Parc concerné** : liste d'items `designation` / `site` / `quantite` rattachés au contrat.

Chaque mutation est commitée dans le dépôt : l'historique Git fait foi (auditabilité).

## Actions

| Action | Paramètres requis | Effet |
|---|---|---|
| `creer` | `reference`, `fournisseur`, `date_debut`, `date_fin` (+ `titre`, `montant`, `description`, `doc_reference`) | Crée le contrat en statut `actif` |
| `lier_marche` | `reference` (contrat), `marche.reference_marche` | Rattache le contrat à un marché (créé si absent) |
| `avenant` | `reference`, `avenant` | Ajoute un avenant et applique ses effets (nouvelle date de fin, nouveau montant) |
| `maj_parc` | `reference`, `operation_parc` (`ajouter`/`retirer`), `items_parc` | Met à jour le parc concerné |
| `cloturer` | `reference`, `motif` (`echeance`, `resiliation`, `reconduction`, `autre`) | Clôture le contrat (refusé si avenant non appliqué) |
| `consulter` | `reference` | Lecture seule du contrat |

Toute action accepte `dry_run: true` (simulation sans écriture) et des champs de ciblage
`owner`, `repo`, `chemin_registre`, `branche` (valeurs par défaut : ce dépôt, branche `main`).

## Exemple d'entrée

```json
{
  "action": "avenant",
  "reference": "C-2025-001",
  "avenant": {
    "reference": "AV-001",
    "type": "prolongation",
    "date_effet": "2026-01-02",
    "nouvelle_date_fin": "2027-01-01",
    "nouveau_montant": 15000.0,
    "doc_reference": "https://ged.example/avenants/AV-001.pdf"
  }
}
```

## Développement

```bash
cd contrats-maintenance/apps/worker
uv sync --all-packages
uv run pytest tests/
```

Le worker découvre automatiquement les workflows de
`contrats-maintenance/apps/worker/src/worker/workflows/`. Déploiement : `mistral apps deploy`
ou déploiement managé depuis le dépôt (app `contrats-maintenance`, module `worker`).
