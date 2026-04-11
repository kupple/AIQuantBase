(function () {
  const canvas = document.getElementById("graph-canvas");
  const details = document.getElementById("graph-details");
  const fitButton = document.getElementById("graph-fit-button");
  const searchInput = document.getElementById("graph-search");

  if (!canvas || typeof cytoscape === "undefined") {
    return;
  }

  const graphElements = JSON.parse(document.getElementById("graph-elements").textContent);
  const nodeLookup = JSON.parse(document.getElementById("graph-node-lookup").textContent);
  const edgeLookup = JSON.parse(document.getElementById("graph-edge-lookup").textContent);
  const nodeCount = graphElements.nodes.length;
  const layoutName = nodeCount <= 12 ? "cose" : "breadthfirst";

  const cy = cytoscape({
    container: canvas,
    elements: [...graphElements.nodes, ...graphElements.edges],
    layout: {
      name: layoutName,
      fit: true,
      padding: 36,
      animate: false,
      spacingFactor: 1.2,
    },
    style: [
      {
        selector: "node",
        style: {
          "background-color": "#1f6f67",
          label: "data(label)",
          color: "#162025",
          "text-wrap": "wrap",
          "text-max-width": 180,
          "text-valign": "bottom",
          "text-margin-y": 12,
          "font-size": 12,
          width: 34,
          height: 34,
          "border-width": 3,
          "border-color": "#d2ebe7",
        },
      },
      {
        selector: "edge",
        style: {
          width: 2.2,
          "line-color": "#88b4ae",
          "target-arrow-color": "#88b4ae",
          "target-arrow-shape": "triangle",
          "curve-style": "bezier",
          label: "data(label)",
          "font-size": 11,
          color: "#4f626a",
          "text-background-color": "#ffffff",
          "text-background-opacity": 0.9,
          "text-background-padding": 3,
        },
      },
      {
        selector: ".faded",
        style: {
          opacity: 0.18,
        },
      },
      {
        selector: ".highlighted",
        style: {
          "background-color": "#e59c2f",
          "line-color": "#e59c2f",
          "target-arrow-color": "#e59c2f",
          opacity: 1,
        },
      },
    ],
  });

  function renderNodeDetail(name) {
    const item = nodeLookup[name];
    if (!item) return;
    details.innerHTML = `
      <div class="graph-details-item">
        <h3>${item.name}</h3>
        <p class="muted">${item.table}</p>
        <div><span class="detail-label">粒度</span><strong>${item.grain}</strong></div>
        <div><span class="detail-label">实体键</span><strong>${item.entity_keys_text || "-"}</strong></div>
        <div><span class="detail-label">时间键</span><strong>${item.time_key || "-"}</strong></div>
        <div><span class="detail-label">出边</span><strong>${item.outgoing_edges.join(", ") || "-"}</strong></div>
        <div><span class="detail-label">入边</span><strong>${item.incoming_edges.join(", ") || "-"}</strong></div>
        <div><span class="detail-label">字段</span><strong>${item.fields_text || "-"}</strong></div>
      </div>
    `;
  }

  function renderEdgeDetail(name) {
    const item = edgeLookup[name];
    if (!item) return;
    details.innerHTML = `
      <div class="graph-details-item">
        <h3>${item.name}</h3>
        <p class="muted">${item.from_node} → ${item.to_node}</p>
        <div><span class="detail-label">关系类型</span><strong>${item.relation_type}</strong></div>
        <div><span class="detail-label">时间模式</span><strong>${item.time_mode || "-"}</strong></div>
        <div><span class="detail-label">Join Keys</span><strong>${item.join_keys_text || "-"}</strong></div>
        <div><span class="detail-label">描述</span><strong>${item.description || "-"}</strong></div>
      </div>
    `;
  }

  cy.on("tap", "node", (event) => {
    renderNodeDetail(event.target.id());
  });

  cy.on("tap", "edge", (event) => {
    renderEdgeDetail(event.target.id());
  });

  fitButton?.addEventListener("click", () => {
    cy.fit(undefined, 36);
  });

  searchInput?.addEventListener("input", () => {
    const keyword = searchInput.value.trim().toLowerCase();
    cy.elements().removeClass("faded highlighted");
    if (!keyword) {
      return;
    }

    const matchedNodes = cy.nodes().filter((node) => {
      const data = node.data();
      return [data.label, data.table, data.grain].join(" ").toLowerCase().includes(keyword);
    });

    cy.elements().addClass("faded");
    matchedNodes.removeClass("faded").addClass("highlighted");
    matchedNodes.connectedEdges().removeClass("faded").addClass("highlighted");
    matchedNodes.connectedEdges().connectedNodes().removeClass("faded");
  });
})();
