package notebook.io

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path, Paths }

import com.typesafe.config.Config
import notebook.{ Notebook, NotebookNotFoundException }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class FileSystemNotebookProviderConfigurator extends Configurable[NotebookProvider] {

  import FileSystemNotebookProviderConfigurator._

  override def apply(config: Config)(implicit ec: ExecutionContext): Future[NotebookProvider] = {
    val rootPath = Future {
      getAbsoluteCanonicalPath(config.getString(NotebooksDir))
    }.recoverWith {
      case t: Throwable =>
        Future.failed(new ConfigurationMissingException(NotebooksDir))
    }

    rootPath.map(new FileSystemNotebookProvider(_))
  }

  private[FileSystemNotebookProviderConfigurator] class FileSystemNotebookProvider(override val root: Path) extends NotebookProvider {
    override def isVersioningSupported: Boolean = false

    override def delete(path: Path)(implicit ev: ExecutionContext): Future[Unit] = {
      Future(FileOperations.recursivelyDeletePath(path))
    }

    override def get(path: Path, version: Option[Version] = None)(implicit ev: ExecutionContext): Future[Notebook] = {
      Future { Files.readAllBytes(path) }.flatMap { bytes =>
        Notebook.deserializeFuture(new String(bytes, StandardCharsets.UTF_8))
      }
    }

    override def save(path: Path, notebook: Notebook, saveSpec: Option[String] = None)(implicit ev: ExecutionContext): Future[Notebook] = {
      Notebook.serializeFuture(notebook).map { nb =>
        Files.write(path, nb.getBytes(StandardCharsets.UTF_8))
      }.map(_ => notebook)
    }

    // Moves the notebook at src Path to the dest Path
    override def moveInternal(src: Path, dest: Path)(implicit ev: ExecutionContext): Future[Path] = Future {
      require(src.toFile.exists(), s"Notebook source at [$src] should exist")
      require(dest.getParent.toFile.exists(), s"Directory at [${dest.getParent}] should exist")
      require(!dest.toFile.exists(), s"Notebook dest at [$dest] should not exist")
      Files.move(src, dest)
      dest
    }

  }
}
object FileSystemNotebookProviderConfigurator {
  val NotebooksDir = "notebooks.dir"

  /**
   * Get absolute path and remove any redundant ./ or ../../
   */
  def getAbsoluteCanonicalPath(path: String): Path = {
    Paths.get(path)
      .toAbsolutePath
      .toFile.getCanonicalFile.toPath
  }
}
