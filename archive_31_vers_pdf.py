#!/usr/bin/env python3
"""
Téléchargeur RAPIDE Archives de Haute-Garonne
Multi-threadé + Split PDF automatique

Dépendances:
    pip install requests Pillow
"""

import requests
import os
from PIL import Image
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
#                           CONFIGURATION
# ============================================================================

EXEMPLE_URL = "https://archives.haute-garonne.fr/archive/download?file=https://archives.haute-garonne.fr/data/files/ad31.diffusion/images/alto/Ged14/Thot/archives_ecrites/1NUM_AC_3201_3400/1_NUM_AC_3217/FRAD031_00001_NUM_AC_003217_0010/FRAD031_00001_NUM_AC_003217_0010_0001.jpg"
NB_PAGES = 249
PAGE_DEBUT = 1
OUTPUT_PDF = "Cadastr17e"  # Sans extension, sera numéroté automatiquement
OUTPUT_DIR = "temp_imagescadastre17e"

# Taille max par PDF en Mo (split automatique)
MAX_PDF_SIZE_MB = 25

# Largeur max des images (pour réduire la taille)
# 2000 = très haute qualité, 1500 = bonne qualité, 1200 = léger
MAX_WIDTH = 2000

# Nombre de threads (3-5 semble OK, 20 se fait bloquer)
NB_THREADS = 20

# ============================================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://archives.haute-garonne.fr/",
}


def analyser_url(url):
    """Détecte le pattern: le numéro est dans le DOSSIER, pas le fichier."""
    url = requests.utils.unquote(url)

    # Pattern: .../XXXX_0008/XXXX_0008_0001.jpg
    match = re.search(r'^(.*?)(\d{4})/([^/]+)(\d{4})(_\d{4}\.jpg)$', url)
    if match:
        return {
            "type": "dossier",
            "prefix": match.group(1),
            "middle": match.group(3),
            "suffix": match.group(5),
            "digits": 4
        }

    # Fallback: pattern simple
    match = re.search(r'^(.*?)(\d{4})(\.jpg)$', url, re.IGNORECASE)
    if match:
        return {
            "type": "simple",
            "prefix": match.group(1),
            "suffix": match.group(3),
            "digits": len(match.group(2))
        }

    return None


def generer_url(pattern, page):
    """Génère l'URL pour une page donnée."""
    if pattern["type"] == "dossier":
        return "{}{:04d}/{}{:04d}{}".format(
            pattern["prefix"],
            page,
            pattern["middle"],
            page,
            pattern["suffix"]
        )
    else:
        return "{}{:0{}d}{}".format(
            pattern["prefix"],
            page,
            pattern["digits"],
            pattern["suffix"]
        )


def telecharger_page(args):
    """Télécharge une seule page (pour thread)."""
    page, url, output_dir = args
    filename = os.path.join(output_dir, "page_{:04d}.jpg".format(page))

    try:
        response = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)

        if response.status_code == 200 and len(response.content) > 10000:
            with open(filename, 'wb') as f:
                f.write(response.content)
            return (page, filename, None)
        else:
            return (page, None, "size={}".format(len(response.content)))
    except Exception as e:
        return (page, None, str(e))


def creer_pdf_split(fichiers, output_base, max_size_mb, max_width):
    """Crée plusieurs PDFs si nécessaire pour respecter la taille max."""

    # Estimer le nombre de pages par PDF
    # En moyenne ~300-400 Ko par page après compression
    pages_par_pdf = int(max_size_mb * 1024 / 350)  # Estimation

    if pages_par_pdf >= len(fichiers):
        # Un seul PDF suffit
        pages_par_pdf = len(fichiers)

    print("   Estimation: ~{} pages par PDF de {} Mo max".format(pages_par_pdf, max_size_mb))

    pdf_num = 1
    idx = 0
    pdfs_crees = []

    while idx < len(fichiers):
        # Charger les images pour ce PDF
        batch_files = fichiers[idx:idx + pages_par_pdf]
        images = []

        print("\n   📄 PDF {} : pages {}-{}...".format(
            pdf_num,
            idx + 1,
            min(idx + pages_par_pdf, len(fichiers))
        ))

        for f in batch_files:
            try:
                img = Image.open(f)

                # Redimensionner si trop grand
                if img.width > max_width:
                    ratio = max_width / img.width
                    new_size = (max_width, int(img.height * ratio))
                    img = img.resize(new_size, Image.LANCZOS)

                if img.mode != 'RGB':
                    img = img.convert('RGB')
                images.append(img)
            except Exception as e:
                print("      ⚠ Erreur image {}: {}".format(f, e))

        if not images:
            idx += pages_par_pdf
            continue

        # Créer le PDF
        pdf_name = "{}_{}.pdf".format(output_base, pdf_num)
        images[0].save(pdf_name, save_all=True, append_images=images[1:], resolution=100, quality=75)

        # Fermer les images
        for img in images:
            img.close()

        # Vérifier la taille
        taille_mo = os.path.getsize(pdf_name) / (1024 * 1024)
        print("      ✓ {} ({:.1f} Mo, {} pages)".format(pdf_name, taille_mo, len(images)))

        pdfs_crees.append((pdf_name, taille_mo, len(images)))

        # Ajuster le nombre de pages pour le prochain PDF si nécessaire
        if taille_mo > max_size_mb * 1.1:  # Plus de 10% au-dessus
            pages_par_pdf = int(pages_par_pdf * max_size_mb / taille_mo)
            print("      ↓ Ajustement: {} pages pour les prochains PDFs".format(pages_par_pdf))
        elif taille_mo < max_size_mb * 0.7:  # Plus de 30% en-dessous
            pages_par_pdf = int(pages_par_pdf * max_size_mb / taille_mo * 0.9)
            print("      ↑ Ajustement: {} pages pour les prochains PDFs".format(pages_par_pdf))

        idx += len(batch_files)
        pdf_num += 1

    return pdfs_crees


def main():
    print("=" * 60)
    print("  📜 Téléchargeur Archives 31 (split {} Mo)".format(MAX_PDF_SIZE_MB))
    print("=" * 60)

    pattern = analyser_url(EXEMPLE_URL)
    if not pattern:
        print("❌ URL invalide")
        return

    print("\n✓ Pattern détecté: {}".format(pattern["type"]))

    # Aperçu des URLs
    print("\n📋 Aperçu des URLs:")
    for p in [1, 2, 3]:
        print("   Page {}: ...{}".format(p, generer_url(pattern, p)[-70:]))

    print("\n✓ {} pages à télécharger\n".format(NB_PAGES))

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Préparer les tâches
    taches = [(page, generer_url(pattern, page), OUTPUT_DIR)
              for page in range(PAGE_DEBUT, PAGE_DEBUT + NB_PAGES)]

    # Téléchargement parallèle
    print("📥 Téléchargement ({} threads)...\n".format(NB_THREADS))

    fichiers = []
    erreurs = []

    with ThreadPoolExecutor(max_workers=NB_THREADS) as executor:
        futures = {executor.submit(telecharger_page, t): t[0] for t in taches}

        done = 0
        for future in as_completed(futures):
            done += 1
            page, filepath, err = future.result()

            if filepath:
                fichiers.append(filepath)
            else:
                erreurs.append((page, err))

            pct = done / NB_PAGES * 100
            bar = '█' * int(pct / 2.5) + '░' * (40 - int(pct / 2.5))
            print("\r   [{}] {:3.0f}% ({}/{})".format(bar, pct, done, NB_PAGES), end="", flush=True)

    print("\n\n📊 Téléchargé: {}/{} pages".format(len(fichiers), NB_PAGES))

    if erreurs:
        print("   ⚠ {} erreurs".format(len(erreurs)))
        for p, e in erreurs[:5]:
            print("      Page {}: {}".format(p, e))

    if not fichiers:
        print("❌ Aucune page téléchargée")
        return

    # Trier les fichiers
    fichiers.sort()

    # Créer les PDFs
    print("\n📄 Création des PDFs (max {} Mo chacun)...".format(MAX_PDF_SIZE_MB))

    pdfs = creer_pdf_split(fichiers, OUTPUT_PDF, MAX_PDF_SIZE_MB, MAX_WIDTH)

    # Résumé
    print("\n" + "=" * 60)
    print("✅ TERMINÉ !")
    print("=" * 60)
    total_mo = sum(p[1] for p in pdfs)
    total_pages = sum(p[2] for p in pdfs)
    print("\n   {} PDFs créés:".format(len(pdfs)))
    for pdf_name, taille, pages in pdfs:
        print("      • {} ({:.1f} Mo, {} pages)".format(pdf_name, taille, pages))
    print("\n   Total: {:.1f} Mo, {} pages".format(total_mo, total_pages))

    # Nettoyage
    print("\n🗑️  Nettoyage des fichiers temporaires...")
    for f in fichiers:
        try:
            os.remove(f)
        except:
            pass
    try:
        os.rmdir(OUTPUT_DIR)
    except:
        pass

    print("\n🎉 Fini !")


if __name__ == "__main__":
    main()