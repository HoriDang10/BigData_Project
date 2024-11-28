
// DOM Elements
const searchInput = document.getElementById('songSearch');
const searchResults = document.getElementById('searchResults');
const likedSongs = document.getElementById('likedSongs');
const recommendedSongs = document.getElementById('recommendedSongs');

// Sample Data
const allSongs = [
  { title: 'QUÁ LÀ TRÔI', artist: 'Bùi Công Nam' },
  { title: 'Lặng', artist: 'Rhymastic' },
  { title: 'Chúa Tể', artist: 'Bùi Công Nam' },
  { title: 'Thuận Nước Đẩy Thuyền', artist: 'S.T Sơn Thạch' },
];

const recommendedSongsList = [
  { title: '12H03', artist: 'Cường Seven, Strong Trọng Hiếu' },
  { title: 'IF', artist: 'Tăng Phúc' },
  { title: 'RƠI', artist: 'S.T Sơn Thạch' },
  { title: 'NÉT', artist: 'Soobin' },
  { title: 'ĐỎ QUÊN ĐI', artist: 'Hà Lê' },
];

// Initialize Recommended Songs Dynamically
function initRecommendedSongs() {
  recommendedSongs.innerHTML = ''; // Clear the list first
  recommendedSongsList.forEach(song => {
    const li = document.createElement('li');
    li.innerHTML = `
      <span>${song.title}</span>
      <span>${song.artist}</span>
    `;
    recommendedSongs.appendChild(li);
  });
}

// Render Search Results Dynamically
function renderSearchResults(query) {
  searchResults.innerHTML = ''; // Clear previous results
  const filteredSongs = allSongs.filter(song =>
    song.title.toLowerCase().includes(query.toLowerCase())
  );

  filteredSongs.forEach(song => {
    const li = document.createElement('li');
    li.innerHTML = `
      <span>${song.title}</span>
      <span>${song.artist}</span>
      <button class="like-btn">
        <img src="https://img.icons8.com/ios/20/000000/like--v1.png" alt="Like">
      </button>
    `;
    // Add like button functionality
    li.querySelector('.like-btn').addEventListener('click', () => addToPlaylist(song));
    searchResults.appendChild(li);
  });
}

// Add Song to Playlist
function addToPlaylist(song) {
  const li = document.createElement('li');
  li.innerHTML = `
    <span>${song.title}</span>
    <span>${song.artist}</span>
  `;
  likedSongs.appendChild(li);
}

// Event Listeners
document.getElementById('searchBtn').addEventListener('click', () => {
  const query = searchInput.value.trim();
  renderSearchResults(query);
});

// Initialize App
initRecommendedSongs();
