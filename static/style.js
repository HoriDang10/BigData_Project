// DOM Elements
const searchInput = document.getElementById("songSearch");
const searchResults = document.getElementById("searchResults");
const selectedSongInput = document.getElementById("selectedSong");
const generatePlaylistBtn = document.getElementById("generatePlaylistBtn");
const generatedPlaylist = document.getElementById("generatedPlaylist");

// Search for Songs Dynamically
function searchSongs() {
  const query = searchInput.value.trim();

  if (!query) {
    searchResults.innerHTML = "<li>Please enter a search term.</li>";
    return;
  }

  searchResults.innerHTML = "<li>Loading...</li>";

  fetch("/search_song", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: query }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        searchResults.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      const results = data.songs || [];
      if (results.length === 0) {
        searchResults.innerHTML = "<li>No matching songs found.</li>";
        return;
      }

      searchResults.innerHTML = "";
      results.forEach((song) => {
        const li = document.createElement("li");
        li.textContent = `${song.title} - ${song.artist}`;
        li.addEventListener("click", () => selectSong(song));
        searchResults.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching search results:", error);
      searchResults.innerHTML = "<li>Error fetching search results.</li>";
    });
}

// Select a Song from Search Results
function selectSong(song) {
  selectedSongInput.value = `${song.title} - ${song.artist}`;
}

// Generate Playlist Based on Selected Song
function generatePlaylist() {
  const selectedSong = selectedSongInput.value.trim();

  if (!selectedSong) {
    generatedPlaylist.innerHTML = "<li>Please paste a song to generate a playlist.</li>";
    return;
  }

  generatedPlaylist.innerHTML = "<li>Loading...</li>";

  fetch("/generate_playlist", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ song: selectedSong }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        generatedPlaylist.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      const playlist = data.playlist || [];
      if (playlist.length === 0) {
        generatedPlaylist.innerHTML = "<li>No songs found for this playlist.</li>";
        return;
      }

      generatedPlaylist.innerHTML = "";
      playlist.forEach((song) => {
        const li = document.createElement("li");
        li.textContent = `${song.title} - ${song.artist}`;
        generatedPlaylist.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error generating playlist:", error);
      generatedPlaylist.innerHTML = "<li>Error generating playlist.</li>";
    });
}

// Event Listeners
document.getElementById("searchBtn").addEventListener("click", searchSongs);
generatePlaylistBtn.addEventListener("click", generatePlaylist);
