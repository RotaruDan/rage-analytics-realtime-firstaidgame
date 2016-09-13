/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.firstaidgame.realtime.utils;

import java.io.Serializable;

/**
 * This class should be used to wrap data required to index a document.
 * 
 * @param <T>
 *            type of the underlying document
 */
public class Document<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the index
	 */
	private String name;
	/**
	 * The type of document
	 */
	private String type;
	/**
	 * The source document
	 */
	private T source;
	/**
	 * The document id
	 */
	private String id;

	private String script;

	public Document(String name, String type, T source, String id) {
		this(name, type, source, id, null);
	}

	public Document(String name, String type, T source, String id, String script) {
		this.name = name;
		this.type = type;
		this.source = source;
		this.id = id;
		this.script = script;
	}

	public String getName() {
		return this.name;
	}

	public String getType() {
		return this.type;
	}

	public T getSource() {
		return this.source;
	}

	public String getId() {
		return this.id;
	}

	public String getScript() {
		return script;
	}

	@Override
	public String toString() {
		String res = "----------\n";
		res += "\tName: " + name + "\n\tType: " + type + "\n\tSource: "
				+ source.toString() + "\n\tId: " + id + "\n\tScript: " + script;
		res += "\n----------";
		return res;
	}
}
