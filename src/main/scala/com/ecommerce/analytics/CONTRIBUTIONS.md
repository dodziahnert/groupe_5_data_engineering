## Transformation et enrichissement

La table des transactions est la table principale : les utilisateurs, produits et marchands sont joints avec des `left join` sur leurs identifiants. Ce choix conserve chaque transaction, même lorsqu'une fiche de reference est absente, afin de ne pas perdre d'evenements dans les indicateurs temporels et de controle.

La transformation ajoute les caracteristiques du timestamp, les rangs et compteurs par utilisateur, les fenetres glissantes de sept jours, le delai depuis le precedent achat et les indicateurs de suspicion. Les moyennes historiques excluent la transaction courante pour eviter qu'elle ne se compare a elle-meme.
