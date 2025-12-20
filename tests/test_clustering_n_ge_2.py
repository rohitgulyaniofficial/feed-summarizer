import pytest


def test_cluster_candidates_returns_empty_for_single_item():
    from tools.merge_report import _cluster_candidates, _should_merge_pair

    items = [
        {
            "id": 1,
            "merge_fp": 123,
            "title_tokens": {"github", "actions", "pricing"},
            "summary_tokens": {"github", "actions", "pricing", "runners"},
        }
    ]

    clusters = _cluster_candidates(items, threshold=24, linkage="complete", eligible=_should_merge_pair)
    assert clusters == []


def test_cluster_candidates_only_returns_clusters_with_n_ge_2():
    from tools.merge_report import _cluster_candidates, _should_merge_pair

    # Two items, but not eligible (no title overlap and no strong summary overlap)
    items = [
        {
            "id": 1,
            "merge_fp": 1,
            "title_tokens": {"irobot"},
            "summary_tokens": {"irobot", "bankruptcy"},
        },
        {
            "id": 2,
            "merge_fp": 1,
            "title_tokens": {"deals", "tvs"},
            "summary_tokens": {"deals", "tvs"},
        },
    ]

    clusters_complete = _cluster_candidates(items, threshold=24, linkage="complete", eligible=_should_merge_pair)
    clusters_single = _cluster_candidates(items, threshold=24, linkage="single", eligible=_should_merge_pair)
    assert clusters_complete == []
    assert clusters_single == []


@pytest.mark.parametrize("linkage", ["single", "complete"])
def test_cluster_candidates_forms_only_n_ge_2_clusters(linkage):
    from tools.merge_report import _cluster_candidates, _should_merge_pair

    # Strong title overlap -> eligible.
    items = [
        {
            "id": 1,
            "merge_fp": 0,
            "title_tokens": {"github", "actions", "pricing"},
            "summary_tokens": {"github", "actions", "pricing", "runners", "hosted", "charging"},
        },
        {
            "id": 2,
            "merge_fp": 0,
            "title_tokens": {"github", "actions", "pricing"},
            "summary_tokens": {"github", "actions", "pricing", "control", "plane", "free"},
        },
    ]

    clusters = _cluster_candidates(items, threshold=0, linkage=linkage, eligible=_should_merge_pair)
    assert clusters == []

    clusters = _cluster_candidates(items, threshold=64, linkage=linkage, eligible=_should_merge_pair)
    assert len(clusters) == 1
    assert len(clusters[0]) >= 2
    assert {c["id"] for c in clusters[0]} == {1, 2}
