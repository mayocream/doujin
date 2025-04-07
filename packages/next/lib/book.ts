export const thumbnail = (id: number) => {
  return `https://s3.doujin.sh/thumbnails/${Math.floor(id / 2000)}/${id}.jpg`
}
