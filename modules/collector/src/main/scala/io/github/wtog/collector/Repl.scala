package io.github.wtog.collector

import java.io.{PrintWriter, StringWriter}
import java.lang.reflect.Method
import java.net.URLDecoder
import java.util

import jline.console.completer.{ArgumentCompleter, Completer}
import notebook.front.Widget
import notebook.repl._
import notebook.util.{InterpreterUtil, Match}
import org.apache.commons.lang3.exception.ExceptionUtils
import scala.tools.nsc.interpreter.Results.{Error, Incomplete => ReplIncomplete, Success => ReplSuccess}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion.{Candidates, ScalaCompleter}
import scala.tools.nsc.interpreter.{IMain, JLineCompletion, JList, Parsed}
import scala.tools.nsc.interpreter.Results.Error
import scala.tools.nsc.interpreter.jline.JLineDelimiter
import scala.util.Try
import scala.util.control.NonFatal
import scala.xml.{NodeSeq, Text}

/**
  * @author : tong.wang
  * @since : 10/2/18 10:26 PM
  * @version : 1.0.0
  */
class Repl(val compilerOpts: List[String], val jars: List[String] = Nil) extends ReplT {
  val LOG = org.slf4j.LoggerFactory.getLogger(classOf[Repl])

  def this() = this(Nil)

  private lazy val stdoutBytes = new ReplOutputStream
  private lazy val stdout = new PrintWriter(stdoutBytes)
  private var _classServerUri: Option[String] = None

  private var _initFinished: Boolean = false
  private var _evalsUntilInitFinished: Int = 0

  def setInitFinished(): Unit = {
    _initFinished = true
  }

  def classServerUri: Option[String] = {
    _classServerUri
  }

  val interp = {
    val settings = new Settings

    settings.embeddedDefaults[Repl]

    if (compilerOpts.nonEmpty) settings.processArguments(compilerOpts, processAll = false)

    val tmp = System.getProperty("java.io.tmpdir")

    // fix for #52
    settings.usejavacp.value = false

    // fix for #52
    val urls: IndexedSeq[String] = {
      import java.io.File
      import java.net.URLClassLoader
      def urls(cl: ClassLoader, acc: IndexedSeq[String] = IndexedSeq.empty): IndexedSeq[String] = {
        if (cl != null) {
          val us = if (!cl.isInstanceOf[URLClassLoader]) {
            acc
          } else {
            acc ++ (cl.asInstanceOf[URLClassLoader].getURLs map { u =>
              val f = new File(u.getFile)
              URLDecoder.decode(f.getAbsolutePath, "UTF8")
            })
          }
          urls(cl.getParent, us)
        } else {
          acc
        }
      }

      val loader = getClass.getClassLoader
      val gurls = urls(loader).distinct //.filter(!_.contains("logback-classic"))//.filter(!_.contains("sbt/"))
      gurls
    }

    val classpath = urls.toList

    //bootclasspath → settings.classpath.isDefault = false → settings.classpath is used
    settings.bootclasspath.value += scala.tools.util.PathResolver.Environment.javaBootClassPath
    settings.bootclasspath.value += java.io.File.pathSeparator + settings.classpath.value

    // LOG the classpath
    // debug the classpath → settings.Ylogcp.value = true

    val fps = jars.map { jar =>

      val f = scala.tools.nsc.io.File(jar).normalize
      //loop.addedClasspath = ClassPath.join(loop.addedClasspath, f.path)
      f.path
    }
    settings.classpath.value = (classpath.distinct ::: fps).mkString(java.io.File.pathSeparator)
    new IMain(settings, stdout)
  }

  private lazy val completion = {
    new JLineCompletion(interp)
  }

  private def scalaToJline(tc: ScalaCompleter): Completer = new Completer {
    def complete(_buf: String, cursor: Int, candidates: JList[CharSequence]): Int = {
      val buf = if (_buf == null) "" else _buf
      val Candidates(newCursor, newCandidates) = tc.complete(buf, cursor)
      newCandidates foreach (candidates add _)
      newCursor
    }
  }

  private lazy val argCompletor = {
    val arg = new ArgumentCompleter(new JLineDelimiter, scalaToJline(completion.completer()))
    arg.setStrict(false)
    arg
  }


  private def listDefinedTerms(request: interp.Request): List[NameDefinition] = {
    request.handlers.flatMap { h =>
      val maybeTerm = h.definesTerm.map(_.encoded)
      val maybeType = h.definesType.map(_.encoded)
      val references = h.referencedNames.map(_.encoded)
      (maybeTerm, maybeType) match {
        case (Some(term), _) =>
          val termType = getTypeNameOfTerm(term).getOrElse("<unknown>")
          Some(NameDefinition(term, termType, references))
        case (_, Some(tpe)) =>
          Some(NameDefinition(tpe, NameDefinition.TYPE_DEFINITION, references))
        case _ => None
      }
    }
  }


  def getTypeNameOfTerm(termName: String): Option[String] = {
    val tpe = try {
      interp.typeOfTerm(termName).toString
    } catch {
      case exc: RuntimeException => println(("Unable to get symbol type", exc)); "<notype>"
    }
    tpe match {
      case "<notype>" => // "<notype>" can be also returned by typeOfTerm
        interp.classOfTerm(termName).map(_.getName)
      case _ =>
        // remove some crap
        Some(
          tpe.replace("iwC$", "")
            .replaceAll("^\\(\\)", "")
        )
    }
  }


  /**
    * Evaluates the given code.  Swaps out the `println` OutputStream with a version that
    * invokes the given `onPrintln` callback everytime the given code somehow invokes a
    * `println`.
    *
    * Uses compile-time implicits to choose a renderer.  If a renderer cannot be found,
    * then just uses `toString` on result.
    *
    * I don't think this is thread-safe (largely because I don't think the underlying
    * IMain is thread-safe), it certainly isn't designed that way.
    *
    * @param code
    * @param onPrintln
    * @return result and a copy of the stdout buffer during the duration of the execution
    */
  def evaluate(code: String,
               onPrintln: String => Unit = _ => (),
               onNameDefinion: NameDefinition => Unit = _ => ()
              ): (EvaluationResult, String) = {
    stdout.flush()
    stdoutBytes.reset()

    // capture stdout if the code the user wrote was a println, for example
    stdoutBytes.aop = onPrintln
    val res = Console.withOut(stdoutBytes) {
      interp.interpret(code)
    }
    stdout.flush()
    stdoutBytes.aop = _ => ()

    val result: EvaluationResult = res match {
      case ReplSuccess =>
        val request = interp.prevRequestList.last
        val lastHandler: interp.memberHandlers.MemberHandler = request.handlers.last

        listDefinedTerms(request).foreach(onNameDefinion)

        try {
          val lastStatementReturnsValue = listDefinedTerms(request).exists(_.name.matches("res[0-9]+"))
          val evalValue = if (lastHandler.definesValue && lastStatementReturnsValue) {
            val line = request.lineRep
            val renderObjectCode =
              """object $rendered {
                |  %s
                |  val rendered: _root_.notebook.front.Widget = try { %s } catch { case t:Throwable => _root_.notebook.front.widgets.html(<div class='alert alert-danger'><div>Exception in implicit renderer: {t.getMessage}</div><pre>{t.getStackTrace.mkString("\n")}</pre></div>) }
                |  %s
                |}""".stripMargin.format(
                request.importsPreamble,
                request.fullPath(lastHandler.definesTerm.get.toString),
                request.importsTrailer
              )
            LOG.debug(renderObjectCode)
            if (line.compile(renderObjectCode)) {
              try {
                val cp = Thread.currentThread().getContextClassLoader
                Thread.currentThread().setContextClassLoader(interp.classLoader)

                val renderedClass2 = Class.forName(line.pathTo("$rendered") + "$", true, interp.classLoader)
                Thread.currentThread().setContextClassLoader(cp)

                def getModule(c: Class[_]) = c.getDeclaredField(interp.global.nme.MODULE_INSTANCE_FIELD.toString).get(())

                val module = getModule(renderedClass2)

                val topInstance = module.getClass.getDeclaredMethod("$iw").invoke(module)

                def iws(o: Class[_], instance: Any): NodeSeq = {
                  val tryClass = o.getName + "$$iw"
                  val o2 = Try {
                    module.getClass.getClassLoader.loadClass(tryClass)
                  }.toOption

                  val maybeIwMethod: Option[Method] = o.getDeclaredMethods.toSeq.find(_.toString.endsWith(".$iw()"))

                  (o2, maybeIwMethod) match {
                    case (Some(o3), _) =>
                      val inst = o.getDeclaredMethod("$iw").invoke(instance)
                      iws(o3, inst)

                    case (None, Some(iwMethod)) =>
                      val inst = iwMethod.invoke(instance)
                      val returnedClassType = iwMethod.getReturnType
                      iws(returnedClassType, inst)

                    case (None, None) =>
                      try {
                        val r = o.getDeclaredMethod("rendered").invoke(instance)
                        val h = r.asInstanceOf[Widget].toHtml
                        h
                      } catch {
                        case e: NoSuchMethodException =>
                          val err = s"Error when rendering cell result: NoSuchMethodException: in ${o.getName} which has such methods: ${o.getDeclaredMethods.toSeq.map(_.toString).sorted}"
                          println(err)
                          LOG.error(err, e)
                          throw e
                      }
                  }
                }

                iws(module.getClass.getClassLoader.loadClass(renderedClass2.getName + "$iw"), topInstance)
              } catch {
                case NonFatal(e) =>
                  e.printStackTrace()
                  LOG.error("Ooops, exception in the cell", e)
                  <span style="color:red;">Ooops, exception in the cell:
                    {e.getMessage}
                  </span>
                    <pre style="color:#999;">
                      {ExceptionUtils.getStackTrace(e)}
                    </pre>
              }
            } else {
              // a line like println(...) is technically a val, but returns null for some reason
              // so wrap it in an option in case that happens...
              Option(line.call("$result")).map { result =>
                Text(
                  try {
                    result.toString
                  }
                  catch {
                    case e: Throwable => "Fail to `toString` the result: " + e.getMessage
                  }
                )
              }.getOrElse(NodeSeq.Empty)
            }
          } else {
            NodeSeq.Empty
          }

          Success(evalValue)
        }
        catch {
          case NonFatal(e) =>
            val ex = new StringWriter()
            e.printStackTrace(new PrintWriter(ex))
            Failure(ex.toString)
        }

      case ReplIncomplete => Incomplete
      case Error => Failure(stdoutBytes.toString)
    }

    if (!_initFinished) {
      _evalsUntilInitFinished = _evalsUntilInitFinished + 1
    }

    (result, stdoutBytes.toString)
  }

  def addCp(newJars: List[String]) = {
    val prevCode = interp.prevRequestList.map(_.originalLine).drop(_evalsUntilInitFinished)
    interp.close()
    val r = new Repl(compilerOpts, newJars ::: jars)
    (r, () => prevCode foreach (c => r.evaluate(c, _ => ())))
  }

  def stop(): Unit = {
    interp.close()
  }
}
