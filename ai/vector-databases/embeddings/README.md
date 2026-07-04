# Embeddings & Cosine Similarity

This project demonstrates the fundamentals of **vector search** by generating **1 million embeddings**, performing a **brute-force cosine similarity search**, and analyzing Spark performance using **Apache Spark on Databricks**.

The goal is to understand how **embeddings** and **cosine similarity** work before exploring **Approximate Nearest Neighbor (ANN)** algorithms such as **HNSW**.

---

## 🎯 What You'll Learn

- What embeddings are
- How cosine similarity measures semantic similarity
- How brute-force vector search works
- Why brute-force search does not scale
- How Spark distributes vector search across executors
- Why modern vector databases use ANN indexing

---

## 📂 Project Structure

```text
embeddings/
│
├── notebooks/
│   └── embeddings-cosine.ipynb
│
├── src/
│   └── embeddings_cosine.py
│
├── benchmark-results/
│
├── requirements.txt
│
└── README.md
```

## 📋 Environment

This project was developed and tested on **Databricks Runtime 15.x**.

### Recommended Environment

- Databricks Runtime 15.x (or later)
- Apache Spark
- Python 3.10+
- NumPy
- Pandas

Databricks already includes Apache Spark and most required libraries, so no additional setup is needed.

---

## 💻 Running Outside Databricks

This project can also be adapted to run locally, but you'll need to install and configure:

- Java (JDK 11 or later)
- Apache Spark 3.5+
- Python 3.10+
- PySpark

After configuring Apache Spark, install the required Python packages:

```bash
pip install -r requirements.txt
```

> **Note:** The notebook uses a few Databricks-specific features (such as `display()` and Delta table APIs). If you're running locally, you may need to replace `display()` with `.show()` and configure Spark appropriately.

---

## ⚙️ Experiment Configuration

| Parameter | Value |
|-----------|-------|
| Dataset Size | 1,000,000 vectors |
| Vector Dimension | 64 |
| Platform | Databricks |
| Runtime | DBR 15.x |
| Language | PySpark |
| Workers | 2 |
| Worker Memory | 16 GB |


---

## 🚀 What the Notebook Does

1. Generates **1 million random embeddings**.
2. Stores the embeddings in a Delta table.
3. Creates a random query vector.
4. Computes cosine similarity against every stored vector.
5. Returns the **Top 10** most similar vectors.
6. Captures Spark job and stage metrics for performance analysis.

---

## 📊 Sample Benchmark

| Metric | Value |
|--------|-------|
| Dataset Size | 1,000,000 vectors |
| Vector Dimension | 64 |
| Input Records | 1,000,000 |
| Input Size | 232 MB |
| Spark Tasks | 8 |
| Wall Clock Time | ~12 seconds (actual query completion time) |
| Executor Runtime | ~100 seconds (combined runtime across all executors for the main stage) |
| Executor CPU Time | ~19.4 seconds |

### Observation

Although Spark distributed the workload across multiple executors, it still scanned **all one million vectors**.

This confirms that brute-force vector search remains an **O(N)** operation, which is why vector databases rely on **Approximate Nearest Neighbor (ANN)** indexes for scalable search.

---

## ▶️ Running the Project

1. Clone this repository.
2. Import the notebook into Databricks **or** run the standalone Python script.
3. Create the `vector_practice` database (or update the database name if preferred).
4. Execute the notebook or script.
5. Review the benchmark results and Spark metrics.

---

## 📁 Repository Contents

| Folder | Description |
|---------|-------------|
| `notebooks/` | Databricks/Jupyter notebooks |
| `src/` | Standalone Python implementation |
| `diagrams/` | Hand-drawn illustrations used in the article |
| `benchmark-results/` | Spark benchmark outputs and performance metrics |

---

## 📝 Related Medium Article

📖 **Why Vector Databases Feel So Fast: Understanding Cosine Similarity and Why Brute-Force Search Does Not Scale**

> *(Link will be added after publication.)*

---

## 🚀 What's Next?

This project is **Part 1** of my **Vector Database Learning Series**.

Upcoming projects include:

- Approximate Nearest Neighbor (ANN)
- HNSW Internals
- FAISS
- Retrieval-Augmented Generation (RAG)
- Vector Search Optimization

---

## 🤝 Contributing

Suggestions, improvements, and feedback are always welcome.

Feel free to:

- Open an Issue
- Submit a Pull Request
- Share ideas for future experiments

---

## ⭐ Support

If you found this project useful, consider giving the repository a **⭐ Star**.

It helps others discover the project and motivates me to continue sharing practical engineering experiments.

---

## 📄 License

This project is licensed under the **MIT License**.
