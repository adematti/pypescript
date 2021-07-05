"""Definition of **pypescript** main function."""

from .pipeline import BasePipeline


def main(config_block=None, pipe_graph_fn=None, data_block=None, save_data_block=None):
    """
    **pypescript** main function.

    Parameters
    ----------
    config_block : string, ConfigBlock, dict, default=None
        If string, path to configuration file.
        Else :class:`ConfigBlock`, dict provided configuration options.
        See :class:`pypeblock.config.ConfigBlock`

    pipe_graph_fn : string, default=None
        If not ``None``, path where to save pipeline graph.
    """
    pipeline = BasePipeline(config_block=config_block,data_block=data_block)
    if pipe_graph_fn is not None:
        pipeline.plot_pipeline_graph(filename=pipe_graph_fn)
    pipeline.setup()
    pipeline.execute()
    if save_data_block is not None:
        pipeline.pipe_block.save(save_data_block)
    pipeline.cleanup()
