"""Proof of concept TopSBM on large corpus"""
from metaflow import FlowSpec, step, batch, S3, Parameter, IncludeFile
import json


class TopSBMFlow(FlowSpec):
    """Fit topic model"""

    n_docs = Parameter(
        "n-docs", default=1_000, type=int, help="Number of documents to train model on"
    )

    input_file = IncludeFile(
        "input-file",
        help="Input JSON file, mapping doc id to doc text. Structure: List[str: str]",
    )

    @step
    def start(self):
        """Fetch abstracts"""

        data = json.loads(self.input_file)
        self.titles = list(data.keys())[: self.n_docs]
        self.texts = [data[k] for k in self.titles]

        print("Number of documents:", len(self.texts))

        self.next(self.make_graph)

    # @batch(memory=60_000, image="metaflow-graph-tool")
    @batch(image="metaflow-graph-tool")
    @step
    def make_graph(self):
        """Build model graph"""
        from sbmtm import sbmtm

        self.model = sbmtm()
        self.model.make_graph(self.texts, documents=self.titles)
        self.next(self.train_model)

    # @batch(memory=125_000, image="metaflow-graph-tool")
    @batch(image="metaflow-graph-tool")
    @step
    def train_model(self):
        """Train topSBM topic model itself"""
        import graph_tool.all as gt

        gt.seed_rng(32)

        self.model.fit()
        self.model = self.model

        print(self.model.topics(l=1, n=20))

        self.next(self.end)

    @step
    def end(self):
        """ """
        # Calls to `self.model` won't work here as graph-tool not installed
        pass


if __name__ == "__main__":
    TopSBMFlow()
