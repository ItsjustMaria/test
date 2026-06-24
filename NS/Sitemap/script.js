let archiveData;

// Load data from JSON
fetch('data.json')
  .then(response => response.json())
  .then(data => {
    archiveData = data;
    // Initialize the default view
    renderHierarchicalView(archiveData);
  })
  .catch(error => {
    console.error('Error loading data:', error);
  });

// ===== Hierarchical View =====
function renderHierarchicalView(data) {
  const treeContainer = document.getElementById('folder-tree');
  treeContainer.innerHTML = '';
  const root = data.archive.items.find(item => item.id === 'root');
  if (!root) return;
  treeContainer.appendChild(renderTreeNode(root, data.archive.items));
}

function renderTreeNode(node, allItems) {
  const li = document.createElement('li');
  if (node.type === 'folder') {
    const span = document.createElement('span');
    span.className = 'folder';
    const toggle = document.createElement('span');
    toggle.className = 'toggle collapsed';
    toggle.addEventListener('click', (e) => {
      e.stopPropagation();
      const children = li.querySelector('.children');
      toggle.classList.toggle('collapsed');
      toggle.classList.toggle('expanded');
      children.classList.toggle('expanded');
    });
    span.appendChild(toggle);
    span.appendChild(document.createTextNode(node.title));
    li.appendChild(span);
    const childrenUl = document.createElement('ul');
    childrenUl.className = 'children';
    node.children.forEach(childId => {
      const childNode = allItems.find(item => item.id === childId);
      if (childNode) childrenUl.appendChild(renderTreeNode(childNode, allItems));
    });
    li.appendChild(childrenUl);
  } else {
    const span = document.createElement('span');
    span.className = 'document';
    span.textContent = node.title;
    span.title = node.description || '';
    li.appendChild(span);
  }
  return li;
}

// ===== Timeline View =====
function renderTimelineView(data) {
  const timelineContainer = document.getElementById('timeline-items');
  timelineContainer.innerHTML = '';
  const datedItems = data.archive.items.filter(item => item.date && item.type !== 'folder');
  datedItems.sort((a, b) => {
    const aDate = a.date.split('-')[0];
    const bDate = b.date.split('-')[0];
    return parseInt(aDate) - parseInt(bDate);
  });
  datedItems.forEach(item => {
    const itemEl = document.createElement('div');
    itemEl.className = 'timeline-item';
    itemEl.setAttribute('data-date', item.date);
    itemEl.innerHTML = `
      <strong>${item.title}</strong>
      <p>${item.description || ''}</p>
      <small>Creator: ${item.creator || 'Unknown'}</small>
    `;
    itemEl.addEventListener('click', () => showItemDetails(item, data.archive.items));
    timelineContainer.appendChild(itemEl);
  });
}

// ===== Grid View =====
function renderGridView(data) {
  const gridContainer = document.getElementById('grid-container');
  gridContainer.innerHTML = '';
  const visualItems = data.archive.items.filter(item => item.thumbnail && item.type !== 'folder');
  visualItems.forEach(item => {
    const itemEl = document.createElement('div');
    itemEl.className = 'grid-item';
    itemEl.innerHTML = `
      <img src="${item.thumbnail}" alt="${item.title}" onerror="this.src='https://via.placeholder.com/150?text=No+Image'">
      <div class="title">${item.title}</div>
      <div class="description">${item.description || ''}</div>
    `;
    itemEl.addEventListener('click', () => showItemDetails(item, data.archive.items));
    gridContainer.appendChild(itemEl);
  });
}

// ===== Graph View =====
let graphScene, graphCamera, graphRenderer, graphControls;
let graphNodes = [];
let graphInfoPanel = null;

function initGraphView() {
  graphInfoPanel = document.createElement('div');
  graphInfoPanel.id = 'graph-info-panel';
  graphInfoPanel.innerHTML = `
    <span class="close" id="graph-close">&times;</span>
    <h3 id="graph-item-title">Item Details</h3>
    <p id="graph-item-description"></p>
    <p><strong>Type:</strong> <span id="graph-item-type"></span></p>
    <p><strong>Subject:</strong> <span id="graph-item-subject"></span></p>
    <p><strong>Object:</strong> <span id="graph-item-object"></span></p>
    <p><strong>Predicate:</strong> <span id="graph-item-predicate"></span></p>
    <p><strong>Relations:</strong> <span id="graph-item-relations"></span></p>
  `;
  document.getElementById('graph-container').appendChild(graphInfoPanel);
  document.getElementById('graph-close').addEventListener('click', () => {
    graphInfoPanel.style.display = 'none';
  });

  const container = document.getElementById('graph-container');
  graphScene = new THREE.Scene();
  graphCamera = new THREE.PerspectiveCamera(75, container.clientWidth / container.clientHeight, 0.1, 1000);
  graphRenderer = new THREE.WebGLRenderer({ antialias: false, alpha: true }); // Disable antialiasing
  graphRenderer.setSize(container.clientWidth, container.clientHeight);
  graphRenderer.setClearColor(0xf0f0f0);
  container.appendChild(graphRenderer.domElement);

  graphControls = new THREE.OrbitControls(graphCamera, graphRenderer.domElement);
  graphControls.enableDamping = true; // Smoother controls
  graphControls.dampingFactor = 0.05;
  graphCamera.position.z = 30;

  // No lighting needed (using MeshBasicMaterial)
  animateGraph();
}

function renderGraphView(data) {
  if (!graphScene) initGraphView();
  // Clear existing nodes
  graphNodes.forEach(node => graphScene.remove(node));
  graphNodes = [];

  // Only show linked-data items in the graph (optional: filter further if needed)
  const items = data.archive.items.filter(item => item.type === 'linked-data');
  const nodeMap = {};

  // Use simpler geometry for spheres
  items.forEach((item, i) => {
    const color = 0x3498db; // Blue for linked-data
    const geometry = new THREE.SphereGeometry(1.5, 8, 8); // Fewer segments
    const material = new THREE.MeshBasicMaterial({ color }); // Basic material (no lighting)
    const node = new THREE.Mesh(geometry, material);
    node.position.x = (Math.random() - 0.5) * 30;
    node.position.y = (Math.random() - 0.5) * 30;
    node.position.z = (Math.random() - 0.5) * 30;
    node.userData = { itemId: item.id };
    graphScene.add(node);
    graphNodes.push(node);
    nodeMap[item.id] = node;
  });

  // Use basic lines (faster than dashed)
  items.forEach(item => {
    if (item.relations) {
      item.relations.forEach(relationId => {
        const sourceNode = nodeMap[item.id];
        const targetNode = nodeMap[relationId];
        if (sourceNode && targetNode) {
          const geometry = new THREE.BufferGeometry();
          const material = new THREE.LineBasicMaterial({ color: 0x95a5a6 });
          const points = [sourceNode.position, targetNode.position];
          geometry.setFromPoints(points);
          const line = new THREE.Line(geometry, material);
          graphScene.add(line);
          graphNodes.push(line);
        }
      });
    }
  });

  // Raycasting for click events
  const raycaster = new THREE.Raycaster();
  const mouse = new THREE.Vector2();
  function onMouseClick(event) {
    mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
    mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
    raycaster.setFromCamera(mouse, graphCamera);
    const intersects = raycaster.intersectObjects(graphNodes.filter(n => n.type === 'Mesh'));
    if (intersects.length > 0) {
      const itemId = intersects[0].object.userData.itemId;
      const item = data.archive.items.find(i => i.id === itemId);
      showGraphItemDetails(item, data.archive.items);
    }
  }
  graphRenderer.domElement.addEventListener('click', onMouseClick, false);
}

  // Add click event via raycasting
  const raycaster = new THREE.Raycaster();
  const mouse = new THREE.Vector2();
  function onMouseClick(event) {
    mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
    mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
    raycaster.setFromCamera(mouse, graphCamera);
    const intersects = raycaster.intersectObjects(graphNodes.filter(n => n.type === 'Mesh'));
    if (intersects.length > 0) {
      const itemId = intersects[0].object.userData.itemId;
      const item = data.archive.items.find(i => i.id === itemId);
      showGraphItemDetails(item, data.archive.items);
    }
  }
  graphRenderer.domElement.addEventListener('click', onMouseClick, false);

  items.forEach(item => {
    if (item.relations) {
      item.relations.forEach(relationId => {
        const sourceNode = nodeMap[item.id];
        const targetNode = nodeMap[relationId];
        if (sourceNode && targetNode) {
          const geometry = new THREE.BufferGeometry();
          const material = new THREE.LineDashedMaterial({ color: 0x95a5a6, dashSize: 1, gapSize: 1 });
          const points = [sourceNode.position, targetNode.position];
          geometry.setFromPoints(points);
          const line = new THREE.Line(geometry, material);
          graphScene.add(line);
          graphNodes.push(line);
        }
      });
    }
  });
}

function showGraphItemDetails(item, allItems) {
  const panel = graphInfoPanel;
  document.getElementById('graph-item-title').textContent = item.title;
  document.getElementById('graph-item-description').textContent = item.description || 'No description';
  document.getElementById('graph-item-type').textContent = item.type;
  document.getElementById('graph-item-subject').textContent = item.subject || 'N/A';
  document.getElementById('graph-item-object').textContent = item.object || 'N/A';
  document.getElementById('graph-item-predicate').textContent = item.predicate || 'N/A';
  document.getElementById('graph-item-relations').textContent =
    item.relations ? item.relations.map(id => {
      const relatedItem = allItems.find(i => i.id === id);
      return relatedItem ? relatedItem.title : id;
    }).join(', ') : 'None';
  panel.style.display = 'block';
}

function animateGraph() {
  requestAnimationFrame(animateGraph);
  graphControls.update();
  graphRenderer.render(graphScene, graphCamera);
}

// ===== Shared Functions =====
function showItemDetails(item, allItems) {
  alert(`Title: ${item.title}\nType: ${item.type}\nDescription: ${item.description || 'None'}`);
}

// ===== View Switching =====
function switchView(viewName, data) {
  document.querySelectorAll('.view').forEach(view => view.classList.remove('active'));
  document.querySelectorAll('.view-option').forEach(opt => opt.classList.remove('active'));
  document.getElementById(`${viewName}-view`).classList.add('active');
  document.querySelector(`.view-option[data-view="${viewName}"]`).classList.add('active');
  switch (viewName) {
    case 'hierarchical':
      renderHierarchicalView(data);
      break;
    case 'timeline':
      renderTimelineView(data);
      break;
    case 'grid':
      renderGridView(data);
      break;
    case 'graph':
      renderGraphView(data);
      break;
  }
}
// ===== Initialize =====
fetch('data.json')
  .then(response => response.json())
  .then(data => {
    archiveData = data;
    // Render the default view
    renderHierarchicalView(archiveData);

    // Set up view switching ONLY after data is loaded
    document.querySelectorAll('.view-option').forEach(option => {
      option.addEventListener('click', () => {
        const viewName = option.getAttribute('data-view');
        switchView(viewName, archiveData);
      });
    });
  })
  .catch(error => {
    console.error('Error loading data:', error);
    alert('Failed to load data. Check the console for errors.');
  });

// Handle window resize for graph view
window.addEventListener('resize', () => {
  if (graphCamera && graphRenderer) {
    const container = document.getElementById('graph-container');
    graphCamera.aspect = container.clientWidth / container.clientHeight;
    graphCamera.updateProjectionMatrix();
    graphRenderer.setSize(container.clientWidth, container.clientHeight);
  }
});