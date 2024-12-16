// DOM Elements
const searchInput = document.getElementById("songSearch");
const searchResults = document.getElementById("searchResults");
const likedSongs = document.getElementById("likedSongs");
const recommendedSongs = document.getElementById("recommendedSongs");
const reloadRecommendationsBtn = document.getElementById("reloadRecommendationsBtn");

// Fetch Initial Recommendations (Top 10 Most Popular Songs)
function fetchInitialRecommendations() {
  recommendedSongs.innerHTML = "<li>Loading...</li>"; // Show loading indicator

  fetch("/recommend_initial")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        recommendedSongs.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      // Display recommended songs
      const recommendations = data.recommended_songs || [];
      if (recommendations.length === 0) {
        recommendedSongs.innerHTML = "<li>No recommendations available.</li>";
        return;
      }

      recommendedSongs.innerHTML = ""; // Clear previous recommendations
      recommendations.forEach((song) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <span>${song.title}</span>
          <span>${song.artist}</span>
          <span>Popularity: ${song.popularity}</span>
        `;
        recommendedSongs.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching initial recommendations:", error);
      recommendedSongs.innerHTML = "<li>Error loading recommendations.</li>";
    });
}

// Search for Songs Dynamically
function searchSongs() {
  const query = searchInput.value.trim();

  if (!query) {
    searchResults.innerHTML = "<li>Please enter a search term.</li>";
    return;
  }

  searchResults.innerHTML = "<li>Loading...</li>"; // Show loading indicator

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

      // Render the search results
      searchResults.innerHTML = ""; // Clear previous results
      results.forEach((song) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <span>${song.title}</span>
          <span>${song.artist}</span>
          <span>Popularity: ${song.popularity}</span>
          <button class="like-btn">
            <img src="https://img.icons8.com/ios/20/000000/like--v1.png" alt="Like">
          </button>
        `;
        li.querySelector(".like-btn").addEventListener("click", () => addToPlaylist(song));
        searchResults.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching search results:", error);
      searchResults.innerHTML = "<li>Error fetching search results.</li>";
    });
}

// Add Song to Playlist
function addToPlaylist(song) {
  const li = document.createElement("li");
  li.innerHTML = `
    <span>${song.title}</span>
    <span>${song.artist}</span>
    <span>Popularity: ${song.popularity}</span>
  `;
  likedSongs.appendChild(li);
}

// Fetch Recommendations Based on Playlist
function fetchRecommendations() {
  const playlistSongs = [...likedSongs.querySelectorAll("li")].map((li) => {
    const title = li.querySelector("span:nth-child(1)").textContent;
    const artist = li.querySelector("span:nth-child(2)").textContent;
    return { title, artist };
  });

  if (playlistSongs.length === 0) {
    fetchInitialRecommendations();
    return;
  }

  fetch("/recommend_based_on_playlist", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ playlist: playlistSongs }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        recommendedSongs.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      const recommendations = data.recommended_songs || [];
      if (recommendations.length === 0) {
        recommendedSongs.innerHTML = "<li>No recommendations available.</li>";
        return;
      }

      recommendedSongs.innerHTML = "";
      recommendations.forEach((song) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <span>${song.title}</span>
          <span>${song.artist}</span>
          <span>Popularity: ${song.popularity}</span>
        `;
        recommendedSongs.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching recommendations:", error);
      recommendedSongs.innerHTML = "<li>Error loading recommendations.</li>";
    });
}

// Event Listeners
document.getElementById("searchBtn").addEventListener("click", searchSongs);
reloadRecommendationsBtn.addEventListener("click", fetchRecommendations);

// Initialize the App
window.onload = fetchInitialRecommendations;
