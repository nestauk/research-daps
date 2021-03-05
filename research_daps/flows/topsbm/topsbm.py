"""Runs a TopSBM topic model (https://topsbm.github.io/) on a corpus of data."""
import json

from metaflow import FlowSpec, IncludeFile, Parameter, step


class TopSBMFlow(FlowSpec):
    """Fit topic model"""

    n_docs = Parameter(
        "n-docs",
        default=1_000,
        type=int,
        help="Number of documents to train model on. -1 uses all documents.",
    )

    input_file = IncludeFile(
        "input-file",
        help="Input JSON file, mapping document id to document tokens. Structure: Dict[str: List[str]]",
    )

    seed = Parameter("seed", default=32, type=int, help="Pseudo-RNG seed.")

    @step
    def start(self):
        """Fetch abstracts"""

        data = json.loads(self.input_file)
        self.titles = list(data.keys())[: self.n_docs]
        self.texts = [data[k] for k in self.titles][: self.n_docs]

        print("Number of documents:", len(self.texts))

        self.next(self.make_graph)

    @step
    def make_graph(self):
        """Build model graph"""
        from sbmtm import sbmtm

        self.model = sbmtm()
        self.model.make_graph(self.texts, documents=self.titles)
        self.next(self.train_model)

    @step
    def train_model(self):
        """Train topSBM topic model itself"""
        import graph_tool.all as gt

        print(self.seed)
        gt.seed_rng(self.seed)

        self.model.fit()
        print("FIT")
        self.model = self.model
        print("Re-assign")

        self.next(self.end)

    @step
    def end(self):
        """ """
        # Calls to `self.model` won't work here as graph-tool not installed
        pass


if __name__ == "__main__":
    TopSBMFlow()
