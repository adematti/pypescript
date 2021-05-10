"""Definition of **pypescript** main function."""

from .module import BasePipeline


def main(config=None, pipe_graph_fn=None):
    """
    **pypescript** main function.

    Parameters
    ----------
    config : string, ConfigBlock, dict, default=None
        If string, path to configuration file.
        Else :class:`ConfigBlock`, dict provided configuration options.
        See :class:`pypeblock.config.ConfigBlock`

    pipe_graph_fn : string, default=None
        If not ``None``, path where to save pipeline graph.
    """
    pipeline = BasePipeline(config_block=config)
    if pipe_graph_fn is not None:
        pipeline.plot_pipeline_graph(filename=pipe_graph_fn)
    pipeline.setup()
    pipeline.execute()
    pipeline.cleanup()
