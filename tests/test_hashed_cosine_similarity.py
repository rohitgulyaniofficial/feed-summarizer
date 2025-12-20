from utils.hashed_cosine import build_hashed_tf_vector, cosine_similarity, hashed_cosine_similarity


def test_hashed_cosine_identical_is_high():
    text = "GitHub Actions pricing changes for self-hosted runners March 2026"
    sim = hashed_cosine_similarity(text, text)
    assert sim > 0.95


def test_hashed_cosine_unrelated_is_low():
    a = "Vintage DRAM tester hardware project"
    b = "Central bank raises interest rates amid inflation concerns"
    sim = hashed_cosine_similarity(a, b)
    assert sim < 0.30


def test_cosine_similarity_is_symmetric():
    a = build_hashed_tf_vector("OpenAI releases new model")
    b = build_hashed_tf_vector("OpenAI unveils new model release")
    assert cosine_similarity(a, b) == cosine_similarity(b, a)
