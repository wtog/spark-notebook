package notebook.client

import notebook.util.Match

sealed trait CalcRequest

case class ExecuteRequest(cellId: String, counter: Int, code: String) extends CalcRequest

case class CompletionRequest(line: String, cursorPosition: Int) extends CalcRequest

case class ObjectInfoRequest(objName: String, position: Int) extends CalcRequest

case object InterruptRequest extends CalcRequest

case class InterruptCellRequest(cellId: String) extends CalcRequest

sealed trait CalcResponse

case class StreamResponse(data: String, name: String) extends CalcResponse

case class ExecuteResponse(outputType: String, content: String, time: String) extends CalcResponse

case class ErrorResponse(message: String, incomplete: Boolean) extends CalcResponse

case class CompletionResponse(cursorPosition: Int, candidates: Seq[Match] = Seq.empty[Match], matchedText: String)

case class ObjectInfoResponse(found: Boolean, name: String, callDef: String, callDocString: String)
