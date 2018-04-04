import quanutils.dataset.pipeline as ppl
import pandas

def executePipeline(args):
	ppl.removeNaNs(pandas.DataFrame())
	return {"result":"Success"}