import semver


def get_latest_version(versions):
    """
    Returns the latest (highest) version from a list of semver strings.

    :param versions: List of version strings (e.g., ["1.0.0", "2.1.0", "1.2.3"]).
    :return: The latest version string.
    """
    # Validate versions using semver.parse_version_info
    valid_versions = []
    for v in versions:
        try:
            valid_versions.append(semver.VersionInfo.parse(v))
        except ValueError:
            continue

    if not valid_versions:
        raise ValueError("No valid semver versions found in the provided list.")

    # Find the maximum version
    latest_version = max(valid_versions)

    return str(latest_version)


def bump_version(version, part):
    """
    Bumps the specified part of a semver string using the semver library.

    :param version: The semver string (e.g., "1.2.3").
    :param part: The part to bump ("major", "minor", "patch").
    :return: The bumped semver string.
    """
    # Parse the version
    parsed_version = semver.VersionInfo.parse(version)

    # Bump the appropriate part
    if part == "major":
        bumped_version = parsed_version.bump_major()
    elif part == "minor":
        bumped_version = parsed_version.bump_minor()
    elif part == "patch":
        bumped_version = parsed_version.bump_patch()
    else:
        raise ValueError("Invalid part. Must be 'major', 'minor', or 'patch'.")

    return str(bumped_version)
