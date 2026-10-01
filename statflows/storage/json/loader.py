# Importation des modules
# Modules de base
from pathlib import Path
from typing import Any, Literal

# Module de chargement de fichiers en local
from .local.loader import load_local

# Module de chargement de fichiers depuis S3 : ne tire pas ``boto3`` au chargement,
# la dépendance de l'extra « s3 » n'est requise qu'à l'établissement de la connexion
from .s3.loader import S3Loader


# Classe générale de chargement des données
class Loader(S3Loader):
    """A unified class for loading JSON data from S3 or local storage.

    Loads from Amazon S3 when ``bucket`` is supplied, from the local filesystem
    otherwise. Only ``.json`` files are supported.

    Args:
        s3_package (str, optional): The package to use for S3 connections
            (``'s3fs'`` or ``'boto3'``). Defaults to ``"boto3"``.

    Attributes:
        s3: The S3 connection object, initialised lazily.

    Examples:
        Load JSON from S3:
        >>> loader = Loader(s3_package='boto3')
        >>> data = loader.load(
        ...     filepath='config/settings.json',
        ...     bucket='my-bucket',
        ...     aws_access_key_id='YOUR_KEY',
        ...     aws_secret_access_key='YOUR_SECRET'
        ... )

        Load JSON from local storage:
        >>> loader = Loader()
        >>> data = loader.load(filepath='config/settings.json')
    """

    # Initialisation
    def __init__(self, s3_package: Literal["boto3", "s3fs"] = "boto3") -> None:
        """Initialize the Loader with specified S3 package.

        Args:
            s3_package (str, optional): Package to use for S3 connections.
                Must be either 's3fs' or 'boto3'. Defaults to "boto3".
        """
        super().__init__(s3_package=s3_package)

    # Méthode de chargement des données
    def load(  # type: ignore[override]
        self,
        filepath: str | Path,
        bucket: str | None = None,
        missing_ok: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Load a JSON file from S3 or local storage.

        Args:
            filepath (str or Path): Path to the JSON file. For S3 this is the
                object key — a ``Path`` is converted to its POSIX form, so a
                registry path built on Windows still addresses the right key;
                for local storage this is the filesystem path.
            bucket (str, optional): S3 bucket name. If ``None``, loads from local
                storage.
            missing_ok (bool, optional): If ``True``, a missing file/object
                returns ``None`` instead of raising. Defaults to  ``False``.
            **kwargs: Additional arguments passed to the underlying loader.
                For S3: ``aws_access_key_id``, ``aws_secret_access_key``,
                ``aws_session_token``, ``endpoint_url``, ``verify``.
                For both: forwarded to ``json.load``.

        Returns:
            Any: The deserialised JSON object, or ``None`` when the file does not
            exist and ``missing_ok`` is ``True``.

        Raises:
            ValueError: If the file extension is not ``.json``.
            FileNotFoundError: If the local file doesn't exist and ``missing_ok``
                is ``False``.
            botocore.exceptions.ClientError: If there are S3 access issues and
                ``missing_ok`` is ``False``.

        Examples:
            Load JSON from S3:
            >>> data = loader.load(
            ...     filepath='data/registry.json',
            ...     bucket='my-bucket',
            ...     aws_access_key_id='KEY',
            ...     aws_secret_access_key='SECRET'
            ... )

            Load local JSON file:
            >>> data = loader.load(filepath='config/settings.json')

            Read a registry that may not exist yet:
            >>> registry = loader.load(
            ...     filepath='registries/last_download.json',
            ...     missing_ok=True,
            ... ) or {}
        """
        # Cas du chargement depuis S3
        if bucket is not None:
            # Extraction de kwargs spécifiques à S3
            s3_kwargs = {
                k: kwargs.pop(k)
                for k in [
                    "aws_access_key_id",
                    "aws_secret_access_key",
                    "aws_session_token",
                    "endpoint_url",
                    "verify",
                ]
                if k in kwargs
            }
            # Connection à S3 si nécessaire
            if not hasattr(self, "s3"):
                self.connect(**s3_kwargs)
            # Normalisation du chemin en clé d'objet S3
            key = Path(filepath).as_posix()
            # Utilise la méthode de chargement depuis S3 du parent
            if not missing_ok:
                return super().load(bucket=bucket, key=key, **kwargs)
            # Absence d'objet détectée via l'exception du client (NoSuchKey/404).
            # Import différé : ``botocore`` arrive avec l'extra « s3 », déjà
            # installé si l'on atteint cette branche (la connexion a réussi).
            from botocore.exceptions import ClientError

            try:
                return super().load(bucket=bucket, key=key, **kwargs)
            except ClientError:
                return None
        # Cas du chargement en local
        else:
            # Court-circuit si le fichier n'existe pas encore
            if missing_ok and not Path(filepath).exists():
                return None
            return load_local(filepath=str(filepath), **kwargs)

    # Méthode de listage des fichiers JSON d'un répertoire (ou préfixe S3)
    def list_json(
        self,
        directory: str | Path,
        bucket: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """List the ``.json`` files directly under a directory or S3 prefix.

        Only direct children are returned (no recursion), and names starting
        with a dot are skipped — the atomic :class:`Saver` writes its
        temporary files as ``.tmp-*.json``, so a write interrupted midway is
        never mistaken for a regular file.

        Args:
            directory (str or Path): Local directory, or S3 prefix (a ``Path``
                is converted to its POSIX form) when ``bucket`` is set.
            bucket (str, optional): S3 bucket name. If ``None``, lists the local
                filesystem.
            **kwargs: S3 connection arguments (``aws_access_key_id``,
                ``aws_secret_access_key``, ``aws_session_token``,
                ``endpoint_url``, ``verify``), used on first connection only.

        Returns:
            List[str]: Sorted paths (local) or object keys (S3) of the JSON
            files, each directly loadable with :meth:`load`. Empty when the
            directory or prefix does not exist.

        Examples:
            >>> Loader().list_json('registries/last_download')  # doctest: +SKIP
            ['registries/last_download/DF_A.json']
        """
        # Cas du stockage local
        if bucket is None:
            path = Path(directory)
            if not path.is_dir():
                return []
            return sorted(
                str(child)
                for child in path.glob("*.json")
                if child.is_file() and not child.name.startswith(".")
            )

        # Connexion à S3 si nécessaire
        if not hasattr(self, "s3"):
            self.connect(**kwargs)
        # Préfixe d'objets S3 terminé par un séparateur (enfants directs seulement)
        prefix = Path(directory).as_posix().rstrip("/") + "/"

        # Récupération des clés selon le package S3
        if self.s3_package == "boto3":
            keys: list[str] = []
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        else:
            keys = [
                path.split("/", 1)[1]
                for path in self.s3.glob(f"{bucket}/{prefix}*.json")
            ]

        # Filtrage des fichiers JSON non masqués
        return sorted(
            key
            for key in keys
            if key.endswith(".json") and not key.rsplit("/", 1)[-1].startswith(".")
        )
