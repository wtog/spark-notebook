package notebook.front.widgets

import play.api.libs.json._

import notebook.Codec
import notebook.JsonCodec._
import notebook.front.SingleConnectedWidget

trait Form[D] extends SingleConnectedWidget[Map[String, String]] {
  def title: String
  def initData: D
  def paramsCodec: Codec[D, Map[String, String]]
  def update: D => D
  def includeAddButton: Boolean = false
  def isShown: Boolean = true

  private[this] val htmlId = "_" + java.util.UUID.randomUUID.toString.replaceAll("\\W", "_")

  private[widgets] var data: D = initData
  implicit val codec: Codec[JsValue, Map[String, String]] = tMap[String]

  currentData.observable.inner.subscribe { m =>
    data = paramsCodec.decode(m)
    update(data) // because the whole reactive `workflow` is not there... yet?
  }

  lazy val toHtml = <div>
                      {
                        scopedScript(
                          s"""req(
          ['observable', 'knockout', 'jquery'],
          function (Observable, ko, $$) {
            var value = Observable.makeObservable(valueId);
            var publishFormData = function(form) {
              var r = $$(form).serializeArray();
              var result = {};
              r.forEach(function(o) {
                result[o.name] = o.value;
              });
              value(result);
            };
            var addEntry = function(form) {
              var entry = $$(form).serializeArray()[0];
              $$("#$htmlId").find("button")
                                     .before("<div class='form-group'><label for-name='"+entry.value+"'>"+entry.value+"</label><input class='form-control' name='"+entry.value+"' value=''/></div>")
            };
            ko.applyBindings({
              formShown:        ko.observable($isShown),
              value:            value,
              publishFormData:  publishFormData,
              addEntry:         addEntry
            }, this);
          }
        )""",
                          Json.obj("valueId" -> dataConnection.id))
                      }
                      <span class="help-block">
                        <input type="checkbox" data-bind="checked: formShown"/><strong>{ title }</strong>
                      </span>
                      <div data-bind="if: formShown">
                        <form role="form" id={ htmlId } data-bind="submit: publishFormData">
                          {
                            paramsCodec.encode(data).map {
                              case (k, v) =>
                                <div class="form-group">
                                  <label for-name={ k }>{ k }</label>
                                  <input name={ k } value={ v } class="form-control"/>
                                </div>
                            }
                          }
                          <button type="submit" class="btn btn-default">Apply</button>
                        </form>
                        <form data-bind="submit: addEntry" role="form" class={ if (includeAddButton) "" else "hide" }>
                          <div class="form-group">
                            <label for-name="add-entry">Add entry</label>
                            <input name="add-entry" class="form-control" type="text"/>
                          </div>
                          <button type="submit" class="btn btn-default">Add</button>
                        </form>
                      </div>
                    </div>
}

object Form {
  def apply[D](
    d: D,
    mainTitle: String,
    paramsToData: Map[String, String] => D,
    dataToParams: D => Map[String, String],
    show: Boolean = true,
    addButton: Boolean = false)(onApply: D => Unit): Form[D] = new Form[D]() {
    override val includeAddButton = addButton
    def initData: D = d
    override val isShown: Boolean = show
    def paramsCodec: notebook.Codec[D, Map[String, String]] =
      new notebook.Codec[D, Map[String, String]] {
        def decode(x: Map[String, String]): D = paramsToData(x)
        def encode(x: D): Map[String, String] = dataToParams(x)
      }
    val title: String = mainTitle
    val update: D => D = m => {
      onApply(m)
      data
    }
  }
}