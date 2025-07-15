import { useState } from "react";

export default function App() {
  const [file, setFile] = useState(null);
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState("");
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(false);

  const BASE_URL = "https://dducuroud0.execute-api.us-east-1.amazonaws.com/dev";

  const uploadFile = async () => {
    if (!file) return;
    const formData = new FormData();
    formData.append("file", file);
    setLoading(true);
    const res = await fetch(`${BASE_URL}/upload`, {
      method: "POST",
      body: formData,
    });
    setLoading(false);
    if (res.ok) alert("File uploaded and indexed!");
    else alert("Upload failed.");
  };

  const ask = async () => {
    if (!question) return;
    setLoading(true);
    const res = await fetch(`${BASE_URL}/ask`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });
    const data = await res.json();
    setAnswer(data.answer);
    setSources(data.sources);
    setLoading(false);
  };

  return (
    <div className="max-w-xl mx-auto p-4 space-y-4">
      <h1 className="text-xl font-bold">🧠 Gen AI RAG App</h1>

      <div className="space-y-2">
        <input type="file" onChange={(e) => setFile(e.target.files[0])} />
        <button
          onClick={uploadFile}
          className="bg-blue-600 text-white px-4 py-2 rounded"
        >
          Upload + Index
        </button>
      </div>

      <div className="space-y-2">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          className="w-full border px-2 py-1"
          placeholder="Ask a question..."
        />
        <button
          onClick={ask}
          className="bg-green-600 text-white px-4 py-2 rounded"
        >
          Ask
        </button>
      </div>

      {loading && <p className="text-gray-600">Loading...</p>}

      {answer && (
        <div className="border p-4 rounded bg-gray-50">
          <p><strong>Answer:</strong> {answer}</p>
          {sources.length > 0 && (
            <ul className="mt-2 text-sm text-gray-600">
              {sources.map((s, i) => (
                <li key={i}>
                  ↳ <code>{s.filename}</code> (chunk {s.chunk})
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
